#!/usr/bin/env python3
"""Интеграционные проверки настоящих HTTP API, PostgreSQL и контейнеров.

Проверяет контракт API, идемпотентность, повторы, отмену, pause/resume воркера,
отказ БД и краткую перегрузку. Запускает Go-тесты хранилища и проверяет валидатор
на намеренно испорченных данных. Работает только с одноразовым Compose-проектом.
"""
import argparse
import concurrent.futures
import json
import datetime
import re
import time
from common import run, sql_quote


def wait_for(fn, timeout=30):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        result = fn()
        if result:
            return result
        time.sleep(.1)
    raise AssertionError('condition timeout')


def integration(h, args):
    h.start(0)
    # Same Go storage functions as production, with an independent disposable DB.
    h.env['TEST_DATABASE_URL'] = 'postgres://soup:' + h.env['POSTGRES_PASSWORD'] + '@db:5432/soup?sslmode=disable'
    output = h.compose('run', '--rm', '--no-deps', '-e', 'TEST_DATABASE_URL', '-e', 'ALLOW_TEST_TRUNCATE=yes',
                      '--entrypoint', '/app/storage.test', 'worker', '-test.v', '-test.count=1')
    (h.directory/'go-storage-tests.log').write_text(output+'\n')
    h.event('storage_scenarios_passed')
    h.sql('TRUNCATE outbox,job_effects,jobs RESTART IDENTITY')

    code, _ = h.request('GET','/jobs',auth=False)
    assert code == 401
    payload = {'model': "literal'); DROP TABLE jobs; --", 'duration_ms': 100, 'max_attempts': 5}
    with concurrent.futures.ThreadPoolExecutor(max_workers=24) as pool:
        jobs = list(pool.map(lambda _: h.post('duplicate', payload), range(48)))
    assert len({j['id'] for j in jobs}) == 1
    jid = jobs[0]['id']
    code, _ = h.request('POST','/jobs',{'model':'conflict'},'duplicate')
    assert code == 409
    assert int(h.sql('SELECT count(*) FROM jobs')) == 1
    assert h.request('POST',f'/jobs/{jid}/cancel')[0] == 200
    assert h.request('POST',f'/jobs/{jid}/cancel')[0] == 200
    assert h.request('GET','/jobs/999999')[0] == 404
    assert h.request('GET','/jobs/not-a-number')[0] == 400
    assert h.request('GET','/jobs?limit=101')[0] == 400
    assert h.request('POST','/jobs',{'model':'x','duration_ms':-1},'bad')[0] == 400
    assert h.request('POST','/jobs',{'model':'x'*5000},'large')[0] == 413
    assert h.request('POST','/jobs',{'model':'x','surprise':1},'unknown')[0] == 400
    for i in range(5):
        h.post(f'page-{i}',{'model':'pagination','duration_ms':10})
    listed, after = [], 0
    while True:
        code, result = h.request('GET',f'/jobs?after={after}&limit=2')
        assert code == 200
        if not result['jobs']:
            break
        listed.extend(j['id'] for j in result['jobs'])
        after = result['next_after']
    assert listed == sorted(set(listed)) and len(listed) == 6
    h.event('http_contract_duplicate_pagination_injection_passed')
    h.compose('up','-d','--no-build','--scale','worker=2','worker')
    h.drain()
    retry = h.post('retry-success',{'model':'retry','duration_ms':10,'max_attempts':3,'fail_until_attempt':2})
    failed = h.post('retry-failure',{'model':'retry','duration_ms':10,'max_attempts':2,'fail_until_attempt':2})
    h.drain()
    assert h.request('GET',f"/jobs/{retry['id']}")[1]['attempts'] == 3
    assert h.request('GET',f"/jobs/{failed['id']}")[1]['status'] == 'failed'

    # Actual double-claim pressure: two processes see one queued row.
    double = h.post('double-process',{'model':'double','duration_ms':3000})
    wait_for(lambda: h.request('GET',f"/jobs/{double['id']}")[1].get('status') == 'running')
    time.sleep(.8)
    assert h.request('GET',f"/jobs/{double['id']}")[1]['attempts'] == 1
    assert h.request('POST',f"/jobs/{double['id']}/cancel")[0] == 200
    h.drain()
    h.event('double_process_claim_and_running_cancel_passed')

    # Freeze the entire container, including Go timers/heartbeat, then run another worker.
    h.compose('up','-d','--no-build','--scale','worker=1','worker')
    zombie = h.post('zombie-process',{'model':'zombie','duration_ms':4000})
    wait_for(lambda: h.request('GET',f"/jobs/{zombie['id']}")[1].get('status') == 'running')
    frozen = h.worker_ids()[0]
    h.owned(frozen)
    h.command(['docker','pause',frozen])
    h.event('zombie_paused',container=frozen,job=zombie['id'])
    try:
        wait_for(lambda: int(h.sql(f"SELECT count(*) FROM jobs WHERE id={zombie['id']} AND lease_until<clock_timestamp()")) == 1)
        # Don't use compose up on the paused container: run a second independent process explicitly.
        rescuer = h.compose('run','-d','--no-deps','worker').splitlines()[-1]
        h.owned(rescuer)
        try:
            wait_for(lambda: h.request('GET',f"/jobs/{zombie['id']}")[1].get('status') == 'succeeded')
            assert h.request('GET',f"/jobs/{zombie['id']}")[1]['attempts'] == 2
            h.command(['docker','unpause',frozen])
            wait_for(lambda: any(f"job_id={zombie['id']} " in line and any(x in line for x in ('ownership lost','finish rejected')) for line in h.command(['docker','logs',frozen], combined=True).splitlines()))
        finally:
            h.owned(rescuer)
            (h.directory/'zombie-rescuer.log').write_text(h.command(['docker','logs','--timestamps',rescuer],combined=True)+'\n')
            h.command(['docker','rm','-f',rescuer])
    finally:
        h.command(['docker','unpause',frozen],check=False)
    h.verify()
    h.event('zombie_resume_fenced')

    # Both process wall clocks are deliberately offset; DB lease calculation is unchanged.
    h.compose('stop','worker')
    skewed = []
    try:
        for offset in ('24h','-24h'):
            cid=h.compose('run','-d','--no-deps','-e',f'TEST_CLOCK_OFFSET={offset}','worker').splitlines()[-1]
            h.owned(cid)
            skewed.append((cid, offset))
        for i in range(8):
            h.post(f'skew-{i}',{'model':'clock','duration_ms':1200})
        h.drain()
        skew_rows=h.rows("SELECT attempts FROM jobs WHERE idempotency_key LIKE 'skew-%'")
        assert len(skew_rows)==8 and all(j['attempts']==1 for j in skew_rows)
        h.verify()
        h.event('clock_offset_passed',offsets=['+24h','-24h'],limitation='injected process wall time, not host clock or DB clock jumps')
    finally:
        for cid, offset in skewed:
            h.owned(cid)
            # Preserve logs from one-off containers before removing them.
            p = __import__('subprocess').run(['docker','logs','--timestamps',cid],capture_output=True,text=True)
            logs = p.stdout+p.stderr
            (h.directory/f'skew-{cid[:12]}.log').write_text(logs)
            wall_times = re.findall(r'process_wall_time=(\S+)', logs)
            assert wall_times, 'skew worker never claimed a job'
            delta = (datetime.datetime.fromisoformat(wall_times[-1]) - datetime.datetime.now(datetime.timezone.utc)).total_seconds()
            expected = 86400 if offset == '24h' else -86400
            assert abs(delta - expected) < 60, (offset, delta)
            h.command(['docker','rm','-f',cid])

    # DB connection loss: kill backend connections during work, then DB restart and retry.
    h.compose('start','worker')
    outage=h.post('db-outage',{'model':'outage','duration_ms':4000,'max_attempts':5})
    wait_for(lambda:h.request('GET',f"/jobs/{outage['id']}")[1].get('status')=='running')
    h.sql("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='soup' AND pid<>pg_backend_pid()")
    h.compose('stop','db')
    assert h.request('GET','/health')[0]==503
    h.compose('start','db')
    wait_for(lambda:h.request('GET','/health')[0]==200)
    # Lost response is modeled by discarding the original POST result and retrying its key.
    assert h.post('db-outage',{'model':'outage','duration_ms':4000,'max_attempts':5})['id']==outage['id']
    h.drain()
    h.verify()
    h.event('db_outage_recovered')

    # Negative control: deliberately corrupt a disposable DB and require the independent
    # validator to reject it. Restore afterwards; preserve the rejected snapshot too.
    h.sql(f"INSERT INTO job_effects(job_id,token,result) SELECT id,token,'{{}}'::jsonb FROM jobs WHERE id={jid}")
    try:
        try:
            h.verify()
        except AssertionError:
            (h.directory/'negative-control-db.json').write_text((h.directory/'final-db.json').read_text())
            h.event('negative_control_detected', invariant='cancelled job must not have an effect')
        else:
            raise AssertionError('validator accepted deliberately inconsistent database')
    finally:
        h.sql(f'DELETE FROM job_effects WHERE job_id={jid}')
    h.verify()
    # Hold a table lock to make 64 handlers occupy their slots; excess HTTP work
    # must get explicit backpressure. This is a brief saturation test, not a soak.
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as locker:
        held = locker.submit(h.sql, "BEGIN; LOCK TABLE jobs IN ACCESS EXCLUSIVE MODE; SELECT pg_sleep(3); COMMIT")
        wait_for(lambda: int(h.sql("SELECT count(*) FROM pg_locks WHERE relation='jobs'::regclass AND mode='AccessExclusiveLock' AND granted")) > 0)
        import threading
        gate = threading.Barrier(96)
        def pressured(i):
            gate.wait(timeout=10)
            return h.request('POST','/jobs',{'model':'pressure'},f'pressure-{i}')
        with concurrent.futures.ThreadPoolExecutor(max_workers=96) as pool:
            outcomes = list(pool.map(pressured, range(96)))
        held.result()
    busy = sum(code == 503 and body.get('error') == 'busy' for code, body in outcomes)
    assert busy > 0, 'admission cap was not exercised'
    assert all(code in (200,201,503) for code, _ in outcomes)
    for i in range(96):
        h.post(f'pressure-{i}',{'model':'pressure'})
    h.drain()
    h.verify()
    h.event('overload_backpressure_passed',busy=busy,total=96)
    h.summary['scenarios']=['double_claim','zombie_pause_resume','cancel_complete_race_go',
                            'duplicate_request','read_committed_go','clock_offset','atomic_rollback_go',
                            'attempt_exhaustion','db_outage','http_validation','sql_injection_literal']


if __name__ == '__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--seed',type=int,default=42)
    run('integration',p.parse_args(),integration)
