#!/usr/bin/env python3
import argparse
import concurrent.futures
import random
import threading
import time
from common import run


def chaos(h, args):
    h.start(args.workers)
    rng = random.Random(args.seed)
    payloads = [{'model': f'model-{i % 7}', 'duration_ms': rng.randint(20, 60), 'max_attempts': 20} for i in range(args.jobs)]
    duplicate_plan = [rng.randrange(args.jobs) for _ in range(args.jobs // 2)]
    cancel_plan = rng.sample(range(args.jobs), args.jobs // 5)
    h.summary['parameters'] = vars(args)
    accepted, lock = {}, threading.Lock()
    stop = threading.Event()
    errors = []
    kills = []
    began = time.monotonic()

    def submit(i):
        key = f'chaos-{i}'
        job = h.post(key, payloads[i])
        with lock:
            if key in accepted and accepted[key] != job['id']:
                raise AssertionError(f'duplicate key produced different ids: {key}')
            accepted[key] = job['id']
        return job

    def duplicates():
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(submit, duplicate_plan))

    def cancellations():
        for i in cancel_plan:
            while not stop.is_set():
                with lock:
                    jid = accepted.get(f'chaos-{i}')
                if jid:
                    code, _ = h.request('POST', f'/jobs/{jid}/cancel')
                    if code not in (200, 409):
                        raise AssertionError(f'cancel failed: {code}')
                    break
                time.sleep(.01)

    def killer():
        randomizer = random.Random(args.seed + 1)
        while not stop.wait(args.kill_interval):
            ids = h.worker_ids()
            assert len(ids) == args.workers, f'expected {args.workers} workers, found {len(ids)}'
            cid = randomizer.choice(sorted(ids))
            h.signal(cid, 'KILL')
            kills.append(cid)
            # A manual Docker kill suppresses restart policy; explicitly restart THIS container.
            h.owned(cid)
            h.command(['docker', 'start', cid])

    def guarded(fn):
        try:
            fn()
        except BaseException as e:
            with lock:
                errors.append(repr(e))
            stop.set()

    threads = [threading.Thread(target=guarded, args=(fn,)) for fn in (duplicates, cancellations, killer)]
    for thread in threads:
        thread.start()
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.clients) as pool:
            list(pool.map(submit, range(args.jobs)))
        for thread in threads[:2]:
            thread.join(timeout=args.timeout)
            assert not thread.is_alive(), 'client thread timeout'
        h.drain(args.timeout)
    finally:
        stop.set()
        for thread in threads:
            thread.join(timeout=30)
    assert not errors, errors
    assert all(not t.is_alive() for t in threads), 'background thread did not stop'
    assert len(accepted) == args.jobs, (len(accepted), args.jobs)
    rows = h.verify(accepted)
    assert kills, 'no worker killed; increase test size/duration'
    assert any(j['attempts'] > 1 for j in rows), 'no recovery observed; increase duration'
    h.summary.update(kills=len(kills), accepted=len(accepted), elapsed_seconds=time.monotonic()-began,
                     duplicate_requests=len(duplicate_plan), cancellation_requests=len(cancel_plan))
    h.event('chaos_verified', **{k: h.summary[k] for k in ('states','kills','effects','reclaimed_jobs','elapsed_seconds')})


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--jobs', type=int, default=10000)
    p.add_argument('--workers', type=int, default=8)
    p.add_argument('--clients', type=int, default=32)
    p.add_argument('--seed', type=int, default=42)
    p.add_argument('--kill-interval', type=float, default=2)
    p.add_argument('--timeout', type=float, default=300)
    args = p.parse_args()
    assert args.jobs > 0 and args.workers > 0 and args.kill_interval > 0
    run('chaos', args, chaos)
