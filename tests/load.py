#!/usr/bin/env python3
"""Нагрузочный тест POST /jobs с заданным расписанием запросов (по умолчанию 100 RPS).

Сохраняет фактический RPS, HTTP-ошибки, p50/p95/p99 и задержку генератора нагрузки.
После запросов ждёт завершения задач и проверяет данные в БД. Скорость HTTP
не равна скорости выполнения задач. С --base-url использует работающий локальный
стенд и отдельный префикс ключей; иначе создаёт одноразовый Compose-проект.
"""
import argparse
import concurrent.futures
import math
import statistics
import time
from common import run
from attached import run_attached


def percentile(values, p):
    return sorted(values)[max(0, math.ceil(len(values)*p)-1)] if values else None


def load(h, args):
    if args.observability and not args.base_url:
        h.env['COMPOSE_PROFILES']='observability'
        h.env['OTEL_EXPORTER_OTLP_ENDPOINT']='http://collector:4318'
        with h.override.open('a') as f:
            f.write("  grafana:\n    ports: !override ['127.0.0.1::3000']\n")
    if args.base_url:
        h.start()  # Reuse existing API/DB/queue; no Docker changes.
    else:
        h.start(args.workers)
    h.summary['parameters'] = vars(args)
    count = round(args.rps * args.duration)
    started = time.monotonic()
    samples = []

    key_prefix = h.prefix if args.base_url else 'load-'

    def send(i, due):
        actual = time.monotonic()
        code, body = h.request('POST', '/jobs', {'model': 'load', 'duration_ms': args.job_ms}, f'{key_prefix}{i}')
        end = time.monotonic()
        return {'i': i, 'code': code, 'id': body.get('id'), 'start': actual-started,
                'latency_ms': (end-actual)*1000, 'scheduled_latency_ms': (end-due)*1000,
                'scheduler_delay_ms': (actual-due)*1000}

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.clients) as pool:
        futures = []
        for i in range(count):
            due = started + i / args.rps
            time.sleep(max(0, due-time.monotonic()))
            futures.append(pool.submit(send, i, due))
        samples = [future.result() for future in futures]
    elapsed = time.monotonic() - started
    ids=h.compose('ps','-q').split()
    h.summary['resources_after_load']=h.command(['docker','stats','--no-stream','--format','{{json .}}',*ids])
    before_drain = h.snapshot()
    snapshot_delay = time.monotonic() - started - elapsed
    h.drain(args.timeout)
    # Reconcile uncertain outcomes using keys in DB; report transport errors separately.
    rows = h.verify()
    by_key = {j['idempotency_key']: j for j in rows}
    accepted = [s for s in samples if s['code'] in (200,201)]
    for s in accepted:
        assert by_key.get(f"{key_prefix}{s['i']}", {}).get('id') == s['id'], f'accepted job lost {s}'
    assert len(rows) <= count and all(j['idempotency_key'].startswith(key_prefix) for j in rows)
    latency = [s['latency_ms'] for s in samples]
    starts = [s['start'] for s in samples]
    h.summary.update(requests=count, successful=len(accepted), errors=count-len(accepted),
                     elapsed_seconds=elapsed, achieved_rps=count/elapsed,
                     backlog_snapshot_delay_seconds=snapshot_delay,
                     start_rate_rps=(count-1)/(max(starts)-min(starts)) if count>1 else None,
                     p50_ms=percentile(latency,.50), p95_ms=percentile(latency,.95), p99_ms=percentile(latency,.99),
                     scheduled_p99_ms=percentile([s['scheduled_latency_ms'] for s in samples],.99),
                     scheduler_p99_ms=percentile([s['scheduler_delay_ms'] for s in samples],.99),
                     completed_at_load_end=sum(j['status']=='succeeded' for j in before_drain),
                     remaining_at_load_end=sum(j['status'] not in ('succeeded','failed','cancelled') for j in before_drain))
    import json
    (h.directory/'samples.json').write_text(json.dumps(samples))
    h.event('load_measured', **{k:h.summary[k] for k in ('achieved_rps','successful','errors','p50_ms','p95_ms','p99_ms','completed_at_load_end','remaining_at_load_end')})
    assert h.summary['achieved_rps'] >= args.rps * .95, 'load generator did not achieve target rate'
    assert h.summary['errors'] == 0, 'HTTP failures observed'


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--observability', action='store_true', help='Create an isolated stack including monitoring (original mode)')
    p.add_argument('--base-url', help='Existing local API, e.g. http://127.0.0.1:8080; do not create containers')
    p.add_argument('--project-name', default='soup_back', help='Existing Docker Compose project for read-only DB verification')
    p.add_argument('--token-env', default='API_TOKEN', help='Name of an environment variable holding the existing API token')
    p.add_argument('--allow-unauthenticated', action='store_true',
                   help='Explicit opt-in: local API accepts requests with no bearer token')
    p.add_argument('--rps', type=float, default=100)
    p.add_argument('--duration', type=float, default=60)
    p.add_argument('--workers', type=int, default=8)
    p.add_argument('--clients', type=int, default=64)
    p.add_argument('--job-ms', type=int, default=40)
    p.add_argument('--timeout', type=float, default=180)
    p.add_argument('--seed', type=int, default=42)
    args = p.parse_args()
    assert args.rps > 0 and args.duration > 0
    if args.base_url and args.observability:
        p.error('--observability starts an isolated stack; omit it when using --base-url')
    if args.base_url:
        run_attached('load', args, load)
    else:
        run('load', args, load)
