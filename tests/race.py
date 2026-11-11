#!/usr/bin/env python3
import argparse
from common import run


def race(h, args):
    with h.override.open('a') as f:
        f.write("  db:\n    ports:\n      - '127.0.0.1::5432'\n")
    h.start(0)
    address=h.compose('port','db','5432').splitlines()[0]
    h.env['TEST_DATABASE_URL']='postgres://soup:'+h.env['POSTGRES_PASSWORD']+'@'+address+'/soup?sslmode=disable'
    h.env['ALLOW_TEST_TRUNCATE']='yes'
    output=h.command(['go','test','-race','-count=1','-v','./internal/storage'])
    (h.directory/'go-race-db.log').write_text(output+'\n')
    h.verify()
    h.event('go_race_with_real_database_passed')


if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--seed',type=int,default=42)
    run('race',p.parse_args(),race)
