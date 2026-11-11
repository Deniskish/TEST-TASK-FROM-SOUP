#!/usr/bin/env python3
"""Record exact combined command output without replacing previous runs."""
import datetime
import pathlib
import subprocess
import sys
import uuid

root = pathlib.Path(__file__).resolve().parents[1]
label, *command = sys.argv[1:]
path = root / 'results' / 'v2' / (datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + label + '-' + uuid.uuid4().hex[:6] + '.log')
print(path, flush=True)
with path.open('wb') as log:
    process = subprocess.Popen(command, cwd=root, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    for line in iter(process.stdout.readline, b''):
        log.write(line)
        log.flush()
        sys.stdout.buffer.write(line)
        sys.stdout.buffer.flush()
    code = process.wait()
path.with_suffix('.exitcode').write_text(str(code) + '\n')
sys.exit(code)
