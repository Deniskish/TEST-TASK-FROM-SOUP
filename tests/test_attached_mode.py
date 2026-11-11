"""Unit-тесты ограничений режима подключения к работающему стенду.
Используют временные файлы и подставные ответы; Docker, БД и настоящий API не нужны.
"""
import os
import pathlib
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import attached
import load


class AttachedModeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.oldroot = attached.ROOT
        attached.ROOT = pathlib.Path(self.tmp.name)
        self.addCleanup(lambda: setattr(attached, 'ROOT', self.oldroot))
        self.addCleanup(self.tmp.cleanup)
        self.env = mock.patch.dict(os.environ, {'API_TOKEN': 'dummy-test-secret'})
        self.env.start()
        self.addCleanup(self.env.stop)

    def harness(self, **kwargs):
        h = attached.AttachedHarness('load', 42, 'http://127.0.0.1:8080',
                                     'soup_back', 'API_TOKEN', **kwargs)
        self.addCleanup(h.close)
        return h

    def test_no_attached_docker_mutation(self):
        """Проверяет отказ для перечисленных опасных Docker/Compose-команд и SQL-записей без разрешения на убийства."""
        h = self.harness()
        for args in [('down', '--volumes'), ('up', '-d'), ('stop', 'worker')]:
            with self.assertRaises(RuntimeError):
                h.compose(*args)
        with self.assertRaises(RuntimeError):
            h.command(['docker', 'kill', 'example-worker'])
        with self.assertRaises(RuntimeError):
            h.command(['docker', 'rm', '-f', 'example-worker'])
        with self.assertRaises(RuntimeError):
            h.sql('DELETE FROM jobs')
        with self.assertRaises(RuntimeError):
            h.sql('SELECT 1; DROP TABLE jobs')

    def test_remote_targets_are_rejected(self):
        """Проверяет, что адрес внешнего HTTPS-сервера отклоняется до начала теста."""
        with self.assertRaises(ValueError):
            attached.AttachedHarness('load', 42, 'https://example.com', 'soup_back', 'API_TOKEN')

    def test_attached_preflight_only_reads(self):
        """На подставном API проверяет, что начальная проверка делает только GET /health и GET /jobs."""
        h = self.harness()
        calls = []
        def fake_request(method, path, *args, **kwargs):
            calls.append((method, path))
            return 200, {'ok': True}
        h.request = fake_request
        h.start()
        self.assertEqual(calls, [('GET', '/health'), ('GET', '/jobs?limit=1')])

    def test_db_snapshot_is_run_scoped(self):
        """Проверяет наличие префикса запуска и таблицы эффектов в SQL снимка; сам запрос в БД не выполняет."""
        h = self.harness()
        with mock.patch.object(h, 'rows', return_value=[]) as rows:
            h.snapshot()
        query = rows.call_args.args[0]
        self.assertIn(h.prefix, query)
        self.assertIn('job_effects', query)

    def test_external_load_keys_are_unique(self):
        """На подставном API проверяет 10 запросов, префикс ключей и учёт ответов; реальную идемпотентность БД не проверяет."""
        # A fake target API isolates this from Docker and validates new-key use.
        class FakeHarness:
            prefix = 'load-attached-faketest-'
            directory = pathlib.Path(self.tmp.name)
            summary = {}
            attached = True
            def __init__(self):
                self.keys = []
                self.jobs = {}
            def start(self):
                pass
            def request(self, method, path, data=None, key=None):
                self.keys.append(key)
                job = {'id': len(self.jobs)+1}
                self.jobs[key] = job
                return 201, job
            def compose(self, *args):
                return ''
            def command(self, *args):
                return ''
            def snapshot(self):
                return self.verify()
            def drain(self, *args):
                pass
            def verify(self):
                return [{'id': job['id'], 'idempotency_key': key, 'status':'succeeded'}
                        for key, job in self.jobs.items()]
            def event(self, *args, **kwargs):
                pass
        from argparse import Namespace
        h = FakeHarness()
        args=Namespace(base_url='http://127.0.0.1:8080', observability=False,
                       rps=50, duration=.2, workers=4, clients=4, job_ms=1, timeout=1,
                       seed=42, project_name='soup_back', token_env='API_TOKEN')
        load.load(h, args)
        self.assertEqual(len(h.keys), 10)
        self.assertTrue(all(key.startswith(h.prefix) for key in h.keys))
        self.assertEqual(h.summary['successful'], 10)
        self.assertEqual(h.summary['errors'], 0)

    def test_token_missing_requires_explicit_opt_in(self):
        """Проверяет отказ при пустом API_TOKEN, если запуск без авторизации явно не разрешён."""
        with mock.patch.dict(os.environ, {'API_TOKEN': ''}):
            with self.assertRaisesRegex(RuntimeError, '--allow-unauthenticated'):
                attached.AttachedHarness('load', 42, 'http://127.0.0.1:8080',
                                         'soup_back', 'API_TOKEN')

    def test_explicit_local_no_auth_sends_no_bearer_header(self):
        """При явном разрешении запуска без токена проверяет отсутствие Authorization в подставном HTTP-запросе."""
        with mock.patch.dict(os.environ, {'API_TOKEN': ''}):
            h = self.harness(allow_unauthenticated=True)
            self.assertEqual(h.summary['auth_mode'], 'unauthenticated-local-opt-in')
            class Response:
                status = 200
                def __enter__(self): return self
                def __exit__(self, *exc): return False
                def read(self): return b'{"jobs": []}'
            with mock.patch('urllib.request.urlopen', return_value=Response()) as fetch:
                code, body = h.request('GET', '/jobs?limit=1')
            self.assertEqual(code, 200)
            self.assertEqual(body, {'jobs': []})
            self.assertIsNone(fetch.call_args.args[0].get_header('Authorization'))


if __name__ == '__main__':
    unittest.main()
