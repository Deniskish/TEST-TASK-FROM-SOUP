"""Unit-тесты самого chaos-сценария: параметры, параллельные клиенты и проверки ошибок.
Используют FakeHarness без Docker и сети. Они не подтверждают восстановление
реальных воркеров и не заменяют прогон 10 000 задач на PostgreSQL/RabbitMQ.
"""
import argparse
import json
import pathlib
import sys
import tempfile
import threading
import time
import unittest

# Can run from SOUP_BACK with: python3 -m unittest discover -s tests -p test_chaos_tz.py -v
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import chaos


class FakeHarness:
    """Модель стенда в памяти: сигналы записываются в список, реальные процессы не запускаются.

    drain сам переводит задачи в succeeded, а verify задаёт условные эффекты и attempts.
    Это входные данные для проверки chaos-клиента, а не результат настоящего обучения или восстановления.
    """
    prefix = 'chaos-attached-unit-'

    def __init__(self, directory, workers=8, inconsistent_duplicate=False):
        self.directory = pathlib.Path(directory)
        self.workers = workers
        self.inconsistent_duplicate = inconsistent_duplicate
        self.lock = threading.Lock()
        self.jobs = {}
        self.id_to_key = {}
        self.calls = {}
        self.kills = []
        self.restarts = []
        self.events = []
        self.summary = {}

    def start(self, workers):
        if workers != self.workers:
            raise RuntimeError('worker preflight failed')

    def sql(self, query):
        return '0'

    def post(self, key, payload):
        time.sleep(.0008)
        with self.lock:
            self.calls[key] = self.calls.get(key, 0) + 1
            if key not in self.jobs:
                self.jobs[key] = {'id': len(self.jobs) + 1, 'status': 'queued'}
                self.id_to_key[self.jobs[key]['id']] = key
            real_id = self.jobs[key]['id']
            if self.inconsistent_duplicate and self.calls[key] > 1:
                return {'id': real_id + 999999}
            return {'id': real_id}

    def request(self, method, path):
        assert method == 'POST' and path.endswith('/cancel')
        jid = int(path.split('/')[2])
        with self.lock:
            key = self.id_to_key[jid]
            job = self.jobs[key]
            if job['status'] in ('queued', 'cancelled'):
                job['status'] = 'cancelled'
                return 200, {'status': 'cancelled'}
            return 409, {'status': job['status']}

    def worker_ids(self):
        return [f'worker-{i}' for i in range(self.workers)]

    def owned(self, cid, service='worker'):
        assert service == 'worker'
        assert cid in self.worker_ids()

    def signal(self, cid, signal):
        self.owned(cid)
        assert signal == 'KILL'
        with self.lock:
            self.kills.append((cid, signal))

    def command(self, args):
        assert args[:2] == ['docker', 'start']
        self.owned(args[-1])
        self.restarts.append(args[-1])

    def drain(self, timeout):
        time.sleep(.09)  # Leave time for multiple fake SIGKILL cycles.
        with self.lock:
            for job in self.jobs.values():
                if job['status'] == 'queued':
                    job['status'] = 'succeeded'

    def verify(self, expected):
        with self.lock:
            actual = {k: job['id'] for k, job in self.jobs.items()}
            assert actual == expected
            states = {s: sum(job['status'] == s for job in self.jobs.values())
                      for s in ('queued', 'running', 'succeeded', 'failed', 'cancelled')}
            rows = [
                {'id': job['id'], 'idempotency_key': key, 'status': job['status'],
                 'attempts': 2 if i == 0 else 1,
                 'effects': 1 if job['status'] == 'succeeded' else 0}
                for i, (key, job) in enumerate(self.jobs.items())
            ]
            self.summary.update(states=states, effects=states['succeeded'],
                                reclaimed_jobs=1)
        return rows

    def event(self, event, **kwargs):
        self.events.append((event, kwargs))


def options(jobs=200, workers=8):
    return argparse.Namespace(jobs=jobs, workers=workers, clients=16, seed=42,
                              kill_interval=.02, timeout=5, base_url='http://127.0.0.1:8080',
                              project_name='soup_back', token_env='API_TOKEN',
                              allow_worker_kills=True, allow_unauthenticated=True)


class ChaosTests(unittest.TestCase):
    def test_tz_defaults(self):
        """Проверяет параметры задания по умолчанию: 10 000 задач, 8 воркеров, интервал SIGKILL 2 секунды."""
        a = chaos.make_parser().parse_args([])
        self.assertEqual(a.jobs, 10000)
        self.assertEqual(a.workers, 8)
        self.assertEqual(a.kill_interval, 2.0)

    def test_parallel_chaos_verification(self):
        """На FakeHarness проверяет 200 задач, 100 повторов, 40 отмен, циклы убийства/запуска и запись журнала событий."""
        with tempfile.TemporaryDirectory() as directory:
            h = FakeHarness(directory)
            chaos.chaos(h, options())
            self.assertEqual(h.summary['accepted'], 200)
            self.assertEqual(h.summary['duplicate_verified'], 100)
            self.assertEqual(h.summary['cancellation_requests'], 40)
            self.assertEqual(h.summary['cancellation_http_200'] + h.summary['cancellation_http_409'], 40)
            self.assertGreaterEqual(h.summary['kills'], 2)
            self.assertEqual(len(h.kills), len(h.restarts))
            self.assertEqual(h.summary['states']['queued'], 0)
            self.assertEqual(h.summary['states']['running'], 0)
            self.assertEqual(h.summary['effects'], h.summary['states']['succeeded'])
            with open(pathlib.Path(directory) / 'kill-events.json') as f:
                self.assertEqual(len(json.load(f)), h.summary['kills'])

    def test_detect_wrong_idempotency_id(self):
        """Подставляет другой ID при повторном POST и проверяет, что chaos-сценарий обнаруживает нарушение."""
        with tempfile.TemporaryDirectory() as directory:
            h = FakeHarness(directory, inconsistent_duplicate=True)
            with self.assertRaises(AssertionError):
                chaos.chaos(h, options(100))

    def test_preexisting_work_blocks_live_kills(self):
        """Имитирует две существующие незавершённые задачи и проверяет отказ до первого убийства воркера."""
        with tempfile.TemporaryDirectory() as directory:
            h = FakeHarness(directory)
            h.sql = lambda query: '2'
            with self.assertRaisesRegex(RuntimeError, 'pre-existing'):
                chaos.chaos(h, options())
            self.assertEqual(h.kills, [])

    def test_wrong_worker_count_blocks_test(self):
        """Имитирует четыре воркера вместо восьми и проверяет отказ начальной проверки без убийств."""
        with tempfile.TemporaryDirectory() as directory:
            h = FakeHarness(directory, workers=4)
            with self.assertRaisesRegex(RuntimeError, 'preflight'):
                chaos.chaos(h, options(workers=8))
            self.assertEqual(h.kills, [])


if __name__ == '__main__':
    unittest.main()
