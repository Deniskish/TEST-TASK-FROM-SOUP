#!/usr/bin/env python3
"""Краткая проверка стандартного Docker Compose: четыре воркера, закрытый порт БД,
API на loopback и успешное выполнение одной задачи с проверкой эффекта в БД.
"""
import argparse
import json
from common import run


def smoke(h, args):
    h.start(None)  # No --scale: use the default compose.yaml four replicas.
    assert len(h.worker_ids()) == 4
    db=h.compose('ps','-q','db')
    h.owned(db, 'db')
    ports=json.loads(h.command(['docker','inspect','--format','{{json .NetworkSettings.Ports}}',db]))
    assert all(not binding for binding in ports.values()), ports
    api=h.compose('ps','-q','api')
    h.owned(api,'api')
    ports=json.loads(h.command(['docker','inspect','--format','{{json .NetworkSettings.Ports}}',api]))
    assert all(item['HostIp']=='127.0.0.1' for items in ports.values() if items for item in items)
    j=h.post('smoke',{'model':'compose-default','duration_ms':20})
    h.drain()
    h.verify({'smoke':j['id']})
    h.event('default_compose_four_workers_and_private_ports_verified')


if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--seed',type=int,default=42)
    run('smoke',p.parse_args(),smoke)
