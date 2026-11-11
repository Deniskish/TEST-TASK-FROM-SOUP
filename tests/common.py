"""Общий стенд для интеграционных, нагрузочных и аварийных тестов.

Создаёт одноразовый Compose-проект, выполняет HTTP/SQL-запросы, сохраняет логи
и проверяет инварианты по БД. Перед сигналом контейнеру сверяет проект и сервис.
Это вспомогательный модуль, а не самостоятельный тест.
"""
import concurrent.futures
import datetime
import json
import os
import pathlib
import platform
import random
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid

ROOT = pathlib.Path(__file__).resolve().parents[1]
TERMINAL = {'succeeded', 'failed', 'cancelled'}


def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def sql_quote(value):
    return "'" + str(value).replace("'", "''") + "'"


class Harness:
    def __init__(self, kind, seed=42):
        self.project = 'soup-test-' + kind + '-' + uuid.uuid4().hex[:10]
        self.directory = ROOT / 'results' / 'v2' / (datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + self.project)
        self.directory.mkdir(parents=True)
        self.seed = seed
        self.env = dict(os.environ)
        # Disregard local deployment configuration; every test gets fresh DB/auth credentials.
        self.env.update(POSTGRES_PASSWORD=uuid.uuid4().hex, API_TOKEN=uuid.uuid4().hex,
                        LEASE_DURATION='2s', HEARTBEAT_INTERVAL='400ms', POLL_INTERVAL='30ms',
                        DB_MAX_CONNS='8')
        self.override = self.directory / 'compose.test.yaml'
        self.override.write_text("services:\n  api:\n    ports: !override\n      - '127.0.0.1::8080'\n")
        self.base = ['docker', 'compose', '--env-file', '/dev/null', '-p', self.project,
                     '-f', str(ROOT / 'compose.yaml'), '-f', str(self.override)]
        self.log_lock = threading.Lock()
        self.trace = (self.directory / 'requests.ndjson').open('a')
        self.started = False
        self.url = ''
        self.summary = {'kind': kind, 'seed': seed, 'project': self.project, 'started_at': utc(),
                        'python': sys.version, 'host': platform.platform(), 'passed': False}
        self.event('run_started', directory=str(self.directory), seed=seed)

    def event(self, event, **data):
        line = json.dumps({'time': utc(), 'event': event, **data}, ensure_ascii=False)
        with self.log_lock:
            print(line, flush=True)
            with (self.directory / 'run.log').open('a') as f:
                f.write(line + '\n')

    def command(self, args, check=True, timeout=180, combined=False):
        p = subprocess.run(args, cwd=ROOT, env=self.env, text=True,
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
        with self.log_lock:
            with (self.directory / 'docker.log').open('a') as f:
                f.write('\n$ ' + ' '.join(args) + '\n')
                f.write(p.stdout)
                f.write(p.stderr)
                f.write('\nexit=' + str(p.returncode) + '\n')
        if check and p.returncode:
            raise RuntimeError(f'command failed ({p.returncode}): {args}: {p.stderr[-2000:]}')
        return (p.stdout + p.stderr if combined else p.stdout).strip()

    def compose(self, *args, **kwargs):
        return self.command(self.base + list(args), **kwargs)

    def start(self, workers=8):
        self.started = True
        if workers is None:
            self.compose('up', '-d', '--no-build', timeout=240)
            workers = 4
        else:
            self.compose('up', '-d', '--no-build', '--scale', f'worker={workers}', timeout=240)
        address = self.compose('port', 'api', '8080').splitlines()[0]
        self.url = 'http://' + address
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            code, _ = self.request('GET', '/health')
            if code == 200:
                break
            time.sleep(.25)
        else:
            raise AssertionError('API readiness timeout')
        self.summary.update(workers=workers, lease='2s', heartbeat='400ms', api_pool=8,
                            worker_pool=8, api_url=self.url,
                            docker_info=self.command(['docker', 'info', '--format', '{{json .ServerVersion}} {{json .OSType}} {{json .Architecture}} CPUs={{.NCPU}} Memory={{.MemTotal}}']))
        # Container configuration (without environment secrets), image IDs and resources.
        ids = self.compose('ps', '-q').split()
        self.summary['resources'] = self.command(['docker', 'stats', '--no-stream', '--format', '{{json .}}', *ids])
        self.summary['image_ids'] = self.compose('images', '--format', 'json')
        self.event('ready', url=self.url, workers=workers)

    def request(self, method, path, body=None, key=None, auth=True):
        data = None if body is None else json.dumps(body).encode()
        headers = {'Content-Type': 'application/json'}
        if auth:
            headers['Authorization'] = 'Bearer ' + self.env['API_TOKEN']
        if key:
            headers['Idempotency-Key'] = key
        req = urllib.request.Request(self.url + path, data=data, headers=headers, method=method)
        start = time.monotonic()
        try:
            with urllib.request.urlopen(req, timeout=8) as response:
                code, raw = response.status, response.read()
        except urllib.error.HTTPError as e:
            code, raw = e.code, e.read()
        except (OSError, TimeoutError) as e:
            code, raw = 0, json.dumps({'transport_error': type(e).__name__}).encode()
        try:
            result = json.loads(raw)
        except ValueError:
            result = {'invalid_json': raw.decode(errors='replace')}
        elapsed = time.monotonic() - start
        with self.log_lock:
            self.trace.write(json.dumps({'time': utc(), 'method': method, 'path': path, 'key': key,
                                         'status': code, 'latency_ms': elapsed * 1000, 'response': result}) + '\n')
            self.trace.flush()
        return code, result

    def post(self, key, payload, retries=6):
        for attempt in range(retries):
            code, body = self.request('POST', '/jobs', payload, key)
            if code in (200, 201):
                return body
            if code not in (0, 503):
                raise AssertionError((code, body))
            time.sleep(min(.1 * 2**attempt, 2))
        raise AssertionError(f'POST retries exhausted: {key}')

    def sql(self, query):
        out = self.compose('exec', '-T', 'db', 'psql', '-X', '-U', 'soup', '-d', 'soup',
                           '-v', 'ON_ERROR_STOP=1', '-At', '-c', query)
        return out

    def rows(self, query):
        return json.loads(self.sql('SELECT coalesce(json_agg(t),\'[]\'::json) FROM (' + query + ') t'))

    def snapshot(self):
        return self.rows('SELECT j.id,j.idempotency_key,j.status,j.attempts,j.max_attempts,j.token, '
                         'j.lease_until, e.token AS effect_token, '
                         '(SELECT count(*) FROM job_effects x WHERE x.job_id=j.id) AS effects '
                         'FROM jobs j LEFT JOIN job_effects e ON e.job_id=j.id ORDER BY j.id')

    def verify(self, expected=None, terminal=True):
        rows = self.snapshot()
        errors = []
        seen = {}
        for j in rows:
            key = j['idempotency_key']
            if key in seen:
                errors.append(f'duplicate key {key}')
            seen[key] = j['id']
            if j['effects'] > 1 or ((j['status'] == 'succeeded') != (j['effects'] == 1)):
                errors.append(f'effect/status mismatch {j}')
            if j['effects'] and j['effect_token'] != j['token']:
                errors.append(f'stale effect {j}')
            if terminal and j['status'] not in TERMINAL:
                errors.append(f'not terminal {j}')
        if expected is not None and seen != expected:
            errors.append(f'accepted mapping differs: expected {len(expected)}, actual {len(seen)}')
        (self.directory / 'final-db.json').write_text(json.dumps(rows, indent=2))
        self.summary['invariant_errors'] = errors
        self.summary['states'] = {state: sum(j['status'] == state for j in rows)
                                  for state in ('queued', 'running', 'succeeded', 'failed', 'cancelled')}
        self.summary['reclaimed_jobs'] = sum(j['attempts'] > 1 for j in rows)
        self.summary['effects'] = sum(j['effects'] for j in rows)
        assert not errors, errors[:10]
        return rows

    def drain(self, timeout=180):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            n = int(self.sql("SELECT count(*) FROM jobs WHERE status IN ('queued','running')"))
            if not n:
                return
            time.sleep(.5)
        self.verify(terminal=True)
        raise AssertionError('drain timeout')

    def worker_ids(self):
        return self.compose('ps', '-q', 'worker').split()

    def owned(self, cid, service='worker'):
        # Never signal arbitrary container IDs: check both project and service labels.
        labels = json.loads(self.command(['docker', 'inspect', '--format', '{{json .Config.Labels}}', cid]))
        assert labels.get('com.docker.compose.project') == self.project
        assert labels.get('com.docker.compose.service') == service

    def signal(self, cid, signal):
        self.owned(cid)
        self.command(['docker', 'kill', '--signal', signal, cid])
        self.event('signal', container=cid, signal=signal)

    def close(self):
        self.summary['ended_at'] = utc()
        try:
            if self.started:
                output = self.compose('logs', '--no-color', '--timestamps', check=False)
                (self.directory / 'containers.log').write_text(output + '\n')
                self.compose('down', '--volumes', '--remove-orphans', timeout=120)
        except BaseException as e:
            self.summary['passed'] = False
            self.summary['cleanup_error'] = str(e)
            raise
        finally:
            self.trace.close()
            (self.directory / 'summary.json').write_text(json.dumps(self.summary, indent=2))
            self.event('run_finished', passed=self.summary['passed'], summary=str(self.directory / 'summary.json'))


def run(kind, args, body):
    h = Harness(kind, args.seed)
    try:
        body(h, args)
        h.summary['passed'] = True
    except BaseException as e:
        import traceback
        h.summary['error'] = str(e)
        with (h.directory / 'failure.log').open('w') as f:
            traceback.print_exc(file=f)
        h.event('failure', error=str(e))
        raise
    finally:
        h.close()
