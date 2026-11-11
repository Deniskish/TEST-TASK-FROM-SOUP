#!/usr/bin/env python3
"""Создаёт сводку V2; исходные логи и результаты V1 не меняет."""
import json
from pathlib import Path
root=Path(__file__).resolve().parents[1]
lines=['# Результаты V2','','Время в именах файлов — UTC. Исходные логи сохранены, включая неуспешные запуски.','', '| Запуск | Успех | Итог |','|---|---|---|']
for p in sorted((root/'results/v2').glob('*/summary.json')):
 s=json.loads(p.read_text());detail=s.get('error',str(s.get('states',{})))
 if s['kind']=='load':detail=f"{s.get('achieved_rps',0):.3f} RPS; p50/p95/p99={s.get('p50_ms',0):.3f}/{s.get('p95_ms',0):.3f}/{s.get('p99_ms',0):.3f} ms; ошибок {s.get('errors')}; мониторинг {s.get('parameters',{}).get('observability',False)}"
 if s['kind']=='chaos':detail+=f"; SIGKILL={s.get('kills')}; повторный claim={s.get('reclaimed_jobs')}"
 lines.append(f"| [{p.parent.name}]({p.parent.name}/summary.json) | {'да' if s['passed'] else 'нет'} | {detail} |")
lines+=['','## Команды','','| Вывод | Код завершения |','|---|---|']
for p in sorted((root/'results/v2').glob('*.exitcode')):lines.append(f'| [{p.stem}]({p.stem}.log) | {p.read_text().strip()} |')
(root/'results/v2/INDEX.md').write_text('\n'.join(lines)+'\n')
