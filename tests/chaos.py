#!/usr/bin/env python3
"""Аварийный тест: по умолчанию 10 000 задач, 8 воркеров и SIGKILL примерно каждые 2 с.

Параллельно отправляет повторные POST и отмены, затем сверяет принятые ID,
конечные состояния, эффекты и fencing tokens с БД. Требует наблюдаемого повторного
claim и хотя бы двух убийств воркеров. Seed фиксирует планы запросов, но не порядок
выполнения потоков. По умолчанию создаёт одноразовый Compose-проект.
С --base-url использует существующий локальный стенд; для убийств воркеров
нужен --allow-worker-kills. Перед стартом проверяет отсутствие ожидающих чужих задач.
"""

import argparse
import concurrent.futures
import json
import random
import threading
import time

from common import run
from attached import run_attached


def chaos(h, args):
    # The attached harness checks that exactly eight live worker containers exist.
    h.start(args.workers)
    if args.base_url:
        # Do not SIGKILL unrelated live work in the user's deployment.
        unrelated = int(h.sql("SELECT count(*) FROM jobs WHERE status IN ('queued','running')"))
        if unrelated:
            raise RuntimeError(
                f'Refusing attached chaos: {unrelated} pre-existing queued/running jobs. '
                'Drain the existing workload or use the isolated chaos test instead.'
            )
    rng = random.Random(args.seed)
    payloads = [
        {
            'model': f'model-{i % 7}',
            'duration_ms': rng.randint(20, 60),
            'max_attempts': 20,  # Survive repeated crashes before exhausting retries.
        }
        for i in range(args.jobs)
    ]
    duplicate_plan = [rng.randrange(args.jobs) for _ in range(args.jobs // 2)]
    cancel_plan = rng.sample(range(args.jobs), args.jobs // 5)
    key_prefix = h.prefix if args.base_url else 'chaos-'

    accepted = {}  # idempotency key -> server-assigned job ID
    lock = threading.Lock()
    stop = threading.Event()
    background_errors = []
    kill_events = []
    cancel_results = {200: 0, 409: 0}
    counters = {'posts': 0, 'duplicate_posts': 0, 'cancels': 0}
    began = time.monotonic()

    h.summary.update(
        parameters=vars(args),
        expected_jobs=args.jobs,
        duplicate_requests_planned=len(duplicate_plan),
        cancellation_requests_planned=len(cancel_plan),
        note='SIGKILL targets only verified worker containers in the selected Compose project.',
    )
    h.event('chaos_started', jobs=args.jobs, workers=args.workers,
            duplicate_posts=len(duplicate_plan), cancel_requests=len(cancel_plan),
            kill_interval_seconds=args.kill_interval, attached=bool(args.base_url))

    def submit(i, duplicate=False):
        key = f'{key_prefix}{i}'
        # h.post retries transient transport/503 errors with the SAME idempotency key.
        job = h.post(key, payloads[i])
        jid = job.get('id')
        if jid is None:
            raise AssertionError(f'POST did not return an ID for {key}: {job}')
        with lock:
            if key in accepted and accepted[key] != jid:
                raise AssertionError(f'Idempotency violation: {key}: {accepted[key]} != {jid}')
            accepted[key] = jid
            counters['posts'] += 1
            if duplicate:
                counters['duplicate_posts'] += 1
            if not duplicate and counters['posts'] % 1000 == 0:
                h.event('posts_progress', completed_requests=counters['posts'],
                        unique_jobs=len(accepted))
        return job

    def duplicate_client():
        # The same keys/payloads race with the primary POST stream.
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(lambda i: submit(i, duplicate=True), duplicate_plan))

    def cancel_one(i):
        key = f'{key_prefix}{i}'
        deadline = time.monotonic() + args.timeout
        while not stop.is_set():
            with lock:
                jid = accepted.get(key)
            if jid is not None:
                code, body = h.request('POST', f'/jobs/{jid}/cancel')
                if code not in (200, 409):
                    raise AssertionError(f'Cancel HTTP {code} for job={jid}: {body}')
                with lock:
                    cancel_results[code] += 1
                    counters['cancels'] += 1
                return
            if time.monotonic() >= deadline:
                raise AssertionError(f'Cancellation waiting for accepted job timed out: {key}')
            stop.wait(.01)
        raise AssertionError(f'Cancellation stopped before job {key} was accepted')

    def cancellation_client():
        # Concurrent cancel requests interleave with creations, duplicates and kills.
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(cancel_one, cancel_plan))

    def worker_killer():
        selector = random.Random(args.seed + 1)
        deadline = time.monotonic() + args.kill_interval
        while not stop.wait(max(0.0, deadline - time.monotonic())):
            ids = sorted(h.worker_ids())
            if len(ids) != args.workers:
                raise AssertionError(f'Expected {args.workers} RUNNING workers, got {len(ids)}')
            cid = selector.choice(ids)
            started_at = time.monotonic()
            h.event('worker_kill_requested', container=cid, elapsed_seconds=started_at-began)
            try:
                # SIGKILL (signal 9), not an ordinary graceful stop.
                h.signal(cid, 'KILL')
            finally:
                # Docker manual SIGKILL can suppress automatic restart.
                # Restore this EXACT worker, after rechecking Compose ownership.
                h.owned(cid, 'worker')
                h.command(['docker', 'start', cid])
            restored_at = time.monotonic()
            with lock:
                kill_events.append({
                    'container': cid,
                    'killed_at_seconds': started_at - began,
                    'restart_duration_seconds': restored_at - started_at,
                })
            h.event('worker_restarted', container=cid,
                    elapsed_seconds=restored_at-began,
                    restart_duration_seconds=restored_at-started_at)
            # Use monotonic deadlines: the target spacing is ~2s and does not
            # accumulate drift after each Docker kill/start operation.
            deadline += args.kill_interval
            if deadline <= restored_at:
                deadline = restored_at + args.kill_interval

    def guarded(name, fn):
        try:
            fn()
        except BaseException as exc:
            with lock:
                background_errors.append(f'{name}: {exc!r}')
            h.event('background_failure', client=name, error=repr(exc))
            stop.set()

    threads = [
        threading.Thread(target=guarded, args=('duplicates', duplicate_client), name='duplicate-client'),
        threading.Thread(target=guarded, args=('cancellations', cancellation_client), name='cancellation-client'),
        threading.Thread(target=guarded, args=('killer', worker_killer), name='worker-killer'),
    ]
    for thread in threads:
        thread.start()

    try:
        # 10,000 unique jobs; duplicates/cancellations/kills run concurrently.
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.clients) as pool:
            list(pool.map(submit, range(args.jobs)))
        for thread in threads[:2]:
            thread.join(timeout=args.timeout)
            if thread.is_alive():
                raise AssertionError(f'{thread.name} exceeded timeout')
        if background_errors:
            raise AssertionError(background_errors)
        # Wait for every job in THIS run to reach a terminal state.
        h.drain(args.timeout)
    finally:
        stop.set()
        for thread in threads:
            thread.join(timeout=30)
        (h.directory / 'kill-events.json').write_text(
            json.dumps(kill_events, indent=2, ensure_ascii=False))
        # Even a failed run must preserve partial counts for the raw audit trail.
        h.summary.update(
            kills=len(kill_events), accepted=len(accepted),
            duplicate_verified=counters['duplicate_posts'],
            cancellation_http_200=cancel_results[200],
            cancellation_http_409=cancel_results[409],
            elapsed_seconds=time.monotonic() - began,
        )

    if background_errors:
        raise AssertionError(background_errors)
    if any(thread.is_alive() for thread in threads):
        raise AssertionError('Background thread did not stop')
    if len(accepted) != args.jobs:
        raise AssertionError(f'Accepted {len(accepted)} unique jobs instead of {args.jobs}')
    if counters['duplicate_posts'] != len(duplicate_plan):
        raise AssertionError(f"Duplicates {counters['duplicate_posts']} != {len(duplicate_plan)}")
    if counters['cancels'] != len(cancel_plan):
        raise AssertionError(f"Cancellations {counters['cancels']} != {len(cancel_plan)}")
    if len(kill_events) < 2:
        raise AssertionError('Fewer than 2 worker SIGKILLs occurred; chaos was not exercised')
    live_workers = h.worker_ids()
    if len(live_workers) != args.workers:
        raise AssertionError(f'Workers did not recover: {len(live_workers)} live of {args.workers}')

    # Verifies mapping of ALL 10k idempotency keys, no lost/duplicate rows,
    # terminal statuses, exactly one stored effect for succeeded jobs and
    # no stored effect for cancelled/failed jobs, and valid fencing tokens.
    rows = h.verify(accepted)
    if len(rows) != args.jobs:
        raise AssertionError(f'DB contains {len(rows)} test rows instead of {args.jobs}')
    if not any(row['attempts'] > 1 for row in rows):
        raise AssertionError('No claim retry observed; worker crash recovery was not demonstrated')
    if h.summary['states']['succeeded'] != h.summary['effects']:
        raise AssertionError('Terminal success count differs from number of effects')

    intervals = [b['killed_at_seconds'] - a['killed_at_seconds']
                 for a, b in zip(kill_events, kill_events[1:])]
    h.summary.update(
        accepted=len(accepted),
        duplicate_requests=len(duplicate_plan),
        duplicate_verified=counters['duplicate_posts'],
        cancellation_requests=len(cancel_plan),
        cancellation_http_200=cancel_results[200],
        cancellation_http_409=cancel_results[409],
        kills=len(kill_events),
        average_kill_spacing_seconds=(sum(intervals) / len(intervals) if intervals else None),
        elapsed_seconds=time.monotonic() - began,
    )
    h.event('chaos_verified', **{k: h.summary[k] for k in (
        'states', 'kills', 'effects', 'reclaimed_jobs', 'accepted',
        'duplicate_verified', 'cancellation_http_200', 'cancellation_http_409',
        'elapsed_seconds')})


def make_parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--base-url', help='Existing LOCAL API; its Grafana monitors this run')
    p.add_argument('--project-name', default='soup_back',
                   help='Exact Docker Compose project containing workers')
    p.add_argument('--token-env', default='API_TOKEN',
                   help='Environment variable with real API token (if used)')
    p.add_argument('--allow-unauthenticated', action='store_true',
                   help='Permit local test requests without an API token')
    p.add_argument('--allow-worker-kills', action='store_true',
                   help='Required explicit permission to SIGKILL existing workers')
    p.add_argument('--jobs', type=int, default=10000)
    p.add_argument('--workers', type=int, default=8)
    p.add_argument('--clients', type=int, default=32)
    p.add_argument('--seed', type=int, default=42)
    p.add_argument('--kill-interval', type=float, default=2.0)
    p.add_argument('--timeout', type=float, default=300)
    return p


def main(argv=None):
    p = make_parser()
    args = p.parse_args(argv)
    if args.jobs < 10 or args.workers < 1 or args.clients < 1 or args.kill_interval <= 0 or args.timeout <= 0:
        p.error('Require jobs>=10, workers>=1, clients>=1, kill-interval>0 and timeout>0')
    if args.base_url:
        if not args.allow_worker_kills:
            p.error('Attached chaos requires --allow-worker-kills (SIGKILLs live workers)')
        run_attached('chaos', args, chaos)
    else:
        if args.allow_worker_kills or args.allow_unauthenticated:
            p.error('--allow-worker-kills / --allow-unauthenticated apply only with --base-url')
        run('chaos', args, chaos)


if __name__ == '__main__':
    main()
