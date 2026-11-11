"""Стенд для тестов уже работающего локального экземпляра Soup.

Не создаёт и не удаляет Compose-ресурсы; проверяет задачи по префиксу запуска.
Убийство и запуск воркеров разрешены только с явным флагом и проверкой принадлежности
контейнера. Нагрузочные запросы создают настоящие задачи и сохраняют их в БД.
Это вспомогательный модуль, а не самостоятельный тест.
"""
import json
import os
import pathlib
import re
import sys
import platform
import time
import urllib.parse
import uuid
import datetime
import threading

from common import Harness, ROOT, utc


class AttachedHarness(Harness):
    def __init__(self, kind, seed, base_url, project_name, token_env, allow_worker_kills=False,
                 allow_unauthenticated=False):
        # Deliberately do not call Harness.__init__: it creates a disposable
        # project and replaces API_TOKEN with a random test credential.
        self.attached = True
        self.seed = seed
        self.project = project_name
        self.allow_worker_kills = allow_worker_kills
        self.url = base_url.rstrip('/')
        parsed = urllib.parse.urlsplit(self.url)
        if (parsed.scheme != 'http' or parsed.hostname not in ('127.0.0.1', 'localhost', '::1')
                or parsed.username or parsed.password or parsed.query or parsed.fragment
                or parsed.path not in ('', '/')):
            raise ValueError('Attached testing is restricted to a local HTTP API on loopback')
        if not re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9_-]*', self.project):
            raise ValueError('Invalid Compose project name')
        self.env = dict(os.environ)
        token = self.env.get(token_env, '')
        if not token and not allow_unauthenticated:
            raise RuntimeError(
                f'API token is missing from environment variable {token_env}. '
                'If your LOCAL API deliberately permits requests without a token, '
                'pass --allow-unauthenticated explicitly; otherwise configure a real API token.'
            )
        self.env['API_TOKEN'] = token
        self.allow_unauthenticated = allow_unauthenticated
        self.prefix = f'{kind}-attached-{uuid.uuid4().hex[:12]}-'
        self.directory = ROOT / 'results' / 'v2' / (
            datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
            + '-' + self.prefix.rstrip('-')
        )
        self.directory.mkdir(parents=True, exist_ok=False)
        self.override = None
        self.base = ['docker', 'compose', '-p', self.project]
        self.log_lock = threading.Lock()
        self.trace = (self.directory / 'requests.ndjson').open('a')
        self.started = False
        self.summary = {
            'kind': kind, 'seed': seed, 'project': self.project, 'attached': True,
            'prefix': self.prefix, 'started_at': utc(), 'api_url': self.url,
            'python': sys.version, 'host': platform.platform(), 'passed': False,
            'auth_mode': 'bearer' if token else 'unauthenticated-local-opt-in',
            'note': 'The existing Compose deployment is NEVER stopped or deleted.'
        }
        self.event('run_started', directory=str(self.directory), attached=True, api_url=self.url,
                   auth_mode=self.summary['auth_mode'])

    def request(self, method, path, body=None, key=None, auth=True):
        # When the local API has no configured token, do not send a fake 'Bearer ' header.
        token_present = bool(self.env.get('API_TOKEN'))
        return super().request(method, path, body, key, auth=auth and token_present)

    def start(self, workers=None):
        # No docker compose up here. The user controls the existing deployment.
        code, body = self.request('GET', '/health')
        if code != 200:
            raise RuntimeError(f'Existing API health check failed: HTTP {code}; response={body}')
        code, body = self.request('GET', '/jobs?limit=1')
        if code != 200:
            raise RuntimeError(f'API authorization/preflight failed: HTTP {code}; response={body}')
        if workers is not None and self.allow_worker_kills:
            # Make sure the URL being tested is published by THIS exact Compose
            # project's API. Otherwise, it is possible to observe one stack
            # while accidentally SIGKILLing workers in another stack.
            mapping = self.compose('port', 'api', '8080').splitlines()[0]
            external_port = int(mapping.rsplit(':', 1)[-1])
            intended_port = urllib.parse.urlsplit(self.url).port
            if intended_port != external_port:
                raise RuntimeError(
                    f'API {self.url} is NOT the published API port ({external_port}) '
                    f'of Compose project {self.project}; refusing worker SIGKILL.'
                )
            api_id = self.compose('ps', '-q', 'api')
            self.owned(api_id, service='api')
        if workers is not None:
            actual = len(self.worker_ids())
            if actual != workers:
                raise RuntimeError(
                    f'Expected {workers} workers in existing project {self.project}, got {actual}. '
                    'Scale them explicitly if this test needs more workers.'
                )
        self.started = True
        self.summary['workers'] = workers
        self.event('ready', attached=True, url=self.url, workers=workers)

    def compose(self, *args, **kwargs):
        # Mutation is never allowed through Compose in attached mode.
        if not args or args[0] not in ('ps', 'port'):
            raise RuntimeError(f'Forbidden Compose operation in attached mode: {args}')
        return super().command(self.base + list(args), **kwargs)

    def command(self, args, **kwargs):
        args = list(args)
        if args[:2] in (['docker', 'kill'], ['docker', 'start']):
            if not self.allow_worker_kills:
                raise RuntimeError('Worker signals on an existing stack require --allow-worker-kills')
            cid = args[-1]
            self.owned(cid, service='worker')
        elif args[:2] in (['docker', 'rm'], ['docker', 'stop'], ['docker', 'restart']):
            raise RuntimeError('Destructive docker operation is forbidden in attached mode')
        elif args[:2] == ['docker', 'compose']:
            # All caller Compose operations must go through the read-only compose() method.
            raise RuntimeError('Use the read-only compose() method')
        return super().command(args, **kwargs)

    def sql(self, query):
        # Only allow a SELECT statement in external DB verification.
        stripped = query.strip()
        if not stripped.lower().startswith('select ') or ';' in stripped:
            raise RuntimeError('Attached SQL access is read-only SELECT only')
        return super().command(
            self.base + ['exec', '-T', 'db', 'psql', '-X', '-U', 'soup', '-d', 'soup',
                         '-v', 'ON_ERROR_STOP=1', '-At', '-c', stripped]
        )

    def snapshot(self):
        # Verify only this run's jobs. Never validate/delete unrelated rows.
        assert re.fullmatch(r'[a-z0-9-]+', self.prefix)
        return self.rows(
            'SELECT j.id,j.idempotency_key,j.status,j.attempts,j.max_attempts,j.token, '
            'j.lease_until,e.token AS effect_token,'
            '(SELECT count(*) FROM job_effects x WHERE x.job_id=j.id) AS effects '
            'FROM jobs j LEFT JOIN job_effects e ON e.job_id=j.id '
            f"WHERE j.idempotency_key LIKE '{self.prefix}%' ORDER BY j.id"
        )

    def drain(self, timeout=180):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            n = int(self.sql(
                'SELECT count(*) FROM jobs '
                f"WHERE idempotency_key LIKE '{self.prefix}%' AND status IN ('queued','running')"
            ))
            if n == 0:
                return
            time.sleep(.5)
        self.verify(terminal=True)
        raise AssertionError('Attached run drain timeout')

    def signal(self, cid, signal):
        if not self.allow_worker_kills:
            raise RuntimeError('Worker signals require --allow-worker-kills')
        if signal != 'KILL':
            raise RuntimeError('Only the worker SIGKILL scenario is supported')
        self.owned(cid, 'worker')
        return super().signal(cid, signal)

    def close(self):
        self.summary['ended_at'] = utc()
        self.trace.close()
        (self.directory / 'summary.json').write_text(json.dumps(self.summary, indent=2))
        self.event('run_finished', passed=self.summary['passed'],
                   summary=str(self.directory / 'summary.json'), deployment_not_removed=True,
                   existing_workers_may_have_been_restarted=self.allow_worker_kills)


def run_attached(kind, args, body):
    """Same error recording as common.run, without Docker cleanup."""
    h = AttachedHarness(kind, args.seed, args.base_url, args.project_name,
                        args.token_env, getattr(args, 'allow_worker_kills', False),
                        getattr(args, 'allow_unauthenticated', False))
    try:
        body(h, args)
        h.summary['passed'] = True
    except BaseException as exc:
        import traceback
        h.summary['error'] = str(exc)
        with (h.directory / 'failure.log').open('w') as f:
            traceback.print_exc(file=f)
        h.event('failure', error=str(exc))
        raise
    finally:
        h.close()
