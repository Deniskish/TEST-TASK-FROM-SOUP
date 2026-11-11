#!/usr/bin/env python3
"""Derived index only. Never rewrites raw run artifacts."""
import json
import pathlib
from collections import Counter
from zoneinfo import ZoneInfo
from datetime import datetime

root=pathlib.Path(__file__).resolve().parents[1]
records=[]
for path in sorted((root/'results').glob('*/summary.json')):
    s=json.loads(path.read_text())
    s['_path']=path.relative_to(root).as_posix()
    stamp=path.parent.name[:16]
    s['_stage']='A' if stamp < '20261111T091700Z' else ('B' if stamp < '20261111T092240Z' else 'C')
    s['_time']=datetime.fromisoformat(s['started_at']).astimezone(ZoneInfo('Europe/Moscow')).strftime('%H:%M:%S')
    records.append(s)
lines=['# Индекс запусков (сводка; исходные файлы сохранены)', '',
       'A: исходные зависимости. B: обновления безопасности. C: итоговый код с ограниченным временем отката.', '',
       '| Начало (МСК, 2026-11-11) | Сборка | Проверка | Успешно | Сводка | Исходные события |',
       '|---|---|---|---|---|---|']
for s in records:
    path=pathlib.Path(s['_path']).relative_to('results')
    lines.append(f"| {s['_time']} | {s['_stage']} | {s['kind']} | {'да' if s['passed'] else 'нет'} | [JSON]({path}) | [лог]({path.parent}/run.log) |")
lines.extend(['','## Коды завершения команд','','| Вывод команды | Код завершения |','|---|---|'])
for p in sorted((root/'results').glob('*.exitcode')):
    lines.append(f'| [{p.stem}]({p.stem}.log) | {p.read_text().strip()} |')
(root/'results'/'INDEX.md').write_text('\n'.join(lines)+'\n')

chaos=['| Прогон / MSK | Сборка | Принято | succeeded | cancelled | failed | SIGKILL | Задач с повторным получением | Секунды |','|---|---|---:|---:|---:|---:|---:|---:|---:|']
loads=['| Прогон / MSK | Сборка | RPS факт | Успех/ошибка | p50, ms | p95, ms | p99, ms | Остаток после нагрузки / завершения обработки |','|---|---|---:|---:|---:|---:|---:|---:|']
for s in records:
    label=f"[{s['_time']}]({s['_path']})"
    if s['kind']=='chaos' and s['passed']:
        state=s['states'];chaos.append(f"| {label} | {s['_stage']} | {s['accepted']} | {state['succeeded']} | {state['cancelled']} | {state['failed']} | {s['kills']} | {s['reclaimed_jobs']} | {s['elapsed_seconds']:.2f} |")
    if s['kind']=='load' and s['passed']:
        loads.append(f"| {label} | {s['_stage']} | {s['achieved_rps']:.3f} | {s['successful']}/{s['errors']} | {s['p50_ms']:.3f} | {s['p95_ms']:.3f} | {s['p99_ms']:.3f} | {s['remaining_at_load_end']} / {s['states']['queued']+s['states']['running']} |")
report=root/'REPORT_RU.md'
if report.exists():
    text=report.read_text()
    for tag,table in [('CHAOS',chaos),('LOAD',loads)]:
        start='<!-- '+tag+'_START -->';end='<!-- '+tag+'_END -->'
        a=text.index(start)+len(start);b=text.index(end)
        text=text[:a]+'\n\n'+'\n'.join(table)+'\n\n'+text[b:]
    report.write_text(text)
print(dict(Counter(s['kind'] for s in records)))
print('all recorded suites passed:',all(s['passed'] for s in records))
