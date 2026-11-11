# Результаты V2

Время в именах файлов — UTC. Исходные логи сохранены, включая неуспешные запуски.

| Запуск | Успех | Итог |
|---|---|---|
| [20261111T131709Z-soup-test-integration-9aa0f15d10](20261111T131709Z-soup-test-integration-9aa0f15d10/summary.json) | да | {'queued': 0, 'running': 0, 'succeeded': 112, 'failed': 1, 'cancelled': 2} |
| [20261111T131915Z-soup-test-race-4e63fbb7eb](20261111T131915Z-soup-test-race-4e63fbb7eb/summary.json) | да | {'queued': 0, 'running': 0, 'succeeded': 1, 'failed': 1, 'cancelled': 1} |
| [20261111T131937Z-soup-test-chaos-94719780df](20261111T131937Z-soup-test-chaos-94719780df/summary.json) | да | {'queued': 0, 'running': 0, 'succeeded': 8155, 'failed': 0, 'cancelled': 1845}; SIGKILL=29; повторный claim=17 |
| [20261111T132100Z-soup-test-load-87d87f55b2](20261111T132100Z-soup-test-load-87d87f55b2/summary.json) | да | 99.989 RPS; p50/p95/p99=2.011/5.400/21.337 ms; ошибок 0; мониторинг False |
| [20261111T132231Z-soup-test-v2-5559c422db](20261111T132231Z-soup-test-v2-5559c422db/summary.json) | нет | <urlopen error [Errno 61] Connection refused> |
| [20261111T132410Z-soup-test-integration-982ddd95e4](20261111T132410Z-soup-test-integration-982ddd95e4/summary.json) | да | {'queued': 0, 'running': 0, 'succeeded': 112, 'failed': 1, 'cancelled': 2} |
| [20261111T132535Z-soup-test-chaos-75d4ff89e6](20261111T132535Z-soup-test-chaos-75d4ff89e6/summary.json) | да | {'queued': 0, 'running': 0, 'succeeded': 8176, 'failed': 0, 'cancelled': 1824}; SIGKILL=27; повторный claim=18 |
| [20261111T132652Z-soup-test-load-a73d06d252](20261111T132652Z-soup-test-load-a73d06d252/summary.json) | да | 99.997 RPS; p50/p95/p99=2.673/17.452/39.973 ms; ошибок 0; мониторинг False |
| [20261111T132727Z-soup-test-v2-59299ab46f](20261111T132727Z-soup-test-v2-59299ab46f/summary.json) | да | {'queued': 0, 'running': 0, 'succeeded': 224, 'failed': 0, 'cancelled': 0} |
| [20261111T132915Z-soup-test-load-04baaf222d](20261111T132915Z-soup-test-load-04baaf222d/summary.json) | да | 99.999 RPS; p50/p95/p99=1.952/2.705/4.297 ms; ошибок 0; мониторинг False |
| [20261111T133028Z-soup-test-load-d66b9e3809](20261111T133028Z-soup-test-load-d66b9e3809/summary.json) | да | 99.998 RPS; p50/p95/p99=1.972/2.821/4.756 ms; ошибок 0; мониторинг True |
| [20261111T133143Z-soup-test-chaos-82c2c6b5e4](20261111T133143Z-soup-test-chaos-82c2c6b5e4/summary.json) | да | {'queued': 0, 'running': 0, 'succeeded': 8171, 'failed': 0, 'cancelled': 1829}; SIGKILL=26; повторный claim=19 |
| [20261111T134950Z-soup-test-v2-9fa6df4bce](20261111T134950Z-soup-test-v2-9fa6df4bce/summary.json) | да | {'queued': 0, 'running': 0, 'succeeded': 226, 'failed': 0, 'cancelled': 0} |
| [20261111T135137Z-soup-test-smoke-4ffa356525](20261111T135137Z-soup-test-smoke-4ffa356525/summary.json) | да | {'queued': 0, 'running': 0, 'succeeded': 1, 'failed': 0, 'cancelled': 0} |
| [20261111T135147Z-soup-test-race-7b8819a6b3](20261111T135147Z-soup-test-race-7b8819a6b3/summary.json) | нет | ["not terminal {'id': 1, 'idempotency_key': 'outbox', 'status': 'queued', 'attempts': 0, 'max_attempts': 5, 'token': 0, 'lease_until': None, 'effect_token': None, 'effects': 0}"] |
| [20261111T135656Z-soup-test-race-434c33cda9](20261111T135656Z-soup-test-race-434c33cda9/summary.json) | да | {'queued': 0, 'running': 0, 'succeeded': 0, 'failed': 0, 'cancelled': 1} |

## Команды

| Вывод | Код завершения |
|---|---|
| [20261111T131559Z-v2-build-0bbb07](20261111T131559Z-v2-build-0bbb07.log) | 0 |
| [20261111T131626Z-docker-build-0572ea](20261111T131626Z-docker-build-0572ea.log) | 0 |
| [20261111T131708Z-integration-92cef8](20261111T131708Z-integration-92cef8.log) | 0 |
| [20261111T131730Z-unit-race-7cc95f](20261111T131730Z-unit-race-7cc95f.log) | 0 |
| [20261111T131741Z-vet-16ed8f](20261111T131741Z-vet-16ed8f.log) | 0 |
| [20261111T131801Z-observability-images-8ddcb6](20261111T131801Z-observability-images-8ddcb6.log) | 0 |
| [20261111T131844Z-metrics-build-a461f4](20261111T131844Z-metrics-build-a461f4.log) | 0 |
| [20261111T131915Z-race-db-b825c6](20261111T131915Z-race-db-b825c6.log) | 0 |
| [20261111T131918Z-publisher-fix-build-ae82d1](20261111T131918Z-publisher-fix-build-ae82d1.log) | 0 |
| [20261111T131937Z-chaos-52bc15](20261111T131937Z-chaos-52bc15.log) | 0 |
| [20261111T132100Z-load-c7eff7](20261111T132100Z-load-c7eff7.log) | 0 |
| [20261111T132159Z-final-image-5d3aa9](20261111T132159Z-final-image-5d3aa9.log) | 0 |
| [20261111T132231Z-broker-observability-123160](20261111T132231Z-broker-observability-123160.log) | 1 |
| [20261111T132236Z-final-race-e0d870](20261111T132236Z-final-race-e0d870.log) | 0 |
| [20261111T132242Z-vulnerabilities-ccaf65](20261111T132242Z-vulnerabilities-ccaf65.log) | 1 |
| [20261111T132358Z-security-update-273729](20261111T132358Z-security-update-273729.log) | 0 |
| [20261111T132402Z-security-scan-repeat-c3c640](20261111T132402Z-security-scan-repeat-c3c640.log) | 0 |
| [20261111T132410Z-integration-repeat-6e985a](20261111T132410Z-integration-repeat-6e985a.log) | 0 |
| [20261111T132416Z-security-build-ca5e84](20261111T132416Z-security-build-ca5e84.log) | 1 |
| [20261111T132510Z-security-build-retry-7c5576](20261111T132510Z-security-build-retry-7c5576.log) | 1 |
| [20261111T132535Z-chaos-repeat-c7d492](20261111T132535Z-chaos-repeat-c7d492.log) | 0 |
| [20261111T132600Z-prom-rules-fcf669](20261111T132600Z-prom-rules-fcf669.log) | 0 |
| [20261111T132630Z-vendor-e315f1](20261111T132630Z-vendor-e315f1.log) | 0 |
| [20261111T132652Z-load-repeat-b211f7](20261111T132652Z-load-repeat-b211f7.log) | 0 |
| [20261111T132653Z-vendored-build-3402bb](20261111T132653Z-vendored-build-3402bb.log) | 0 |
| [20261111T132727Z-broker-observability-repeat-501ed5](20261111T132727Z-broker-observability-repeat-501ed5.log) | 0 |
| [20261111T132915Z-final-load-basic-0908bb](20261111T132915Z-final-load-basic-0908bb.log) | 0 |
| [20261111T133027Z-final-load-observability-6561dd](20261111T133027Z-final-load-observability-6561dd.log) | 0 |
| [20261111T133143Z-final-chaos-8894db](20261111T133143Z-final-chaos-8894db.log) | 0 |
| [20261111T134950Z-alert-rule-tests-74d66f](20261111T134950Z-alert-rule-tests-74d66f.log) | 0 |
| [20261111T134950Z-final-v2-af810c](20261111T134950Z-final-v2-af810c.log) | 0 |
| [20261111T135136Z-final-smoke-671434](20261111T135136Z-final-smoke-671434.log) | 0 |
| [20261111T135147Z-final-database-race-f3b6cf](20261111T135147Z-final-database-race-f3b6cf.log) | 1 |
| [20261111T135349Z-alert-rules-positive-negative-dcafb0](20261111T135349Z-alert-rules-positive-negative-dcafb0.log) | 0 |
| [20261111T135350Z-final-vet-b5d75b](20261111T135350Z-final-vet-b5d75b.log) | 0 |
| [20261111T135350Z-vps-config-e4d740](20261111T135350Z-vps-config-e4d740.log) | 0 |
| [20261111T135656Z-final-database-race-repeat-c82985](20261111T135656Z-final-database-race-repeat-c82985.log) | 0 |
| [20261111T135740Z-final-unit-race-128704](20261111T135740Z-final-unit-race-128704.log) | 0 |
