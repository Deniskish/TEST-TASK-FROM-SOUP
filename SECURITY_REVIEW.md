# Минимальная проверка безопасности

Область: просмотр Go-кода, SQL и конфигурации Compose, HTTP/DB integration checks,
`govulncheck`. Это не тест на проникновение, не аудит образов ОС и не доказательство
отсутствия уязвимостей. Подробные результаты и исходные логи — в `REPORT_RU.md`.

| Риск | Реализовано / проверено | Остаточный риск |
|---|---|---|
| SQL injection | pgx `$n` для всех пользовательских значений; SQL identifiers не приходят из API. Integration передал `literal'); DROP TABLE jobs; --` как строку модели, таблица сохранилась. | Один payload не покрывает все атаки; безопасность зависит также от драйвера/режима протокола. |
| Валидация | model, duration, attempts, positive ID, bounded limit, strict JSON fields; unit + HTTP integration. | Duplicate JSON keys: последнее значение; null scalar → zero, документировано. |
| Большие тела | MaxBytesReader 4096; HTTP test получил 413. | TCP connection floods и все варианты slowloris не тестировались. |
| Таймауты | Headers/read/write/idle, request context, statement/lock/idle-in-transaction timeouts. | Подбор значений для реальной сети/ML-нагрузки не сделан. |
| Соединения | pgx pool 8/process, PostgreSQL max_connections=150; 64 active HTTP handlers. | Излишнее масштабирование процессов может исчерпать DB budget. |
| Секреты | Environment, gitignored `.env*`, VPS требует пароль и token. | Docker admin видит env; это не secret manager. Старый volume не меняет пароль автоматически. |
| Логи | Go не пишет body, bearer, DATABASE_URL или полный DB error; только IDs/token/состояние. | Полные DB diagnostics намеренно не попадают в API-логи, это ограничивает диагностику. PostgreSQL может логировать failing SQL; настройки production redaction не разработаны. |
| Перегрузка | Реальный тест блокировал jobs table: из 96 запросов 32 получили `503 busy`; после снятия lock запросы повторены теми же ключами, инварианты проверены. | Нет лимита общего backlog, квот, ограничения числа TCP connections на proxy или распределённого rate limiter. Длительная перегрузка не проверена. |
| Ошибки | Клиенту generic 503, не SQL/credentials. Same-key retry; на отмену/неверный ввод отдельные коды. | Неопределённый COMMIT outcome принципиально возможен; нужно повторное чтение/тот же ключ. |
| Доступ к API | Constant-time comparison SHA256 bearer, 401 проверен. Compose loopback; VPS требует token. `/health` публичен. | Локальный default без auth. Один token даёт полный доступ ко всем заданиям; нет tenants/roles, TLS и per-user audit. |
| Доступ к БД | В обычном/VPS Compose вообще нет published DB port, собственная Compose network. | Первоначальная роль `soup` — superuser от official image. Production требует отдельную app role с минимальными grants и миграционную роль. |
| Контейнеры | API/worker non-root, read-only root FS, cap_drop ALL, no-new-privileges. Никакого Docker socket в сервисах. | Harness имеет Docker privileges на машине тестировщика; запускать только доверенный код harness. Образы Alpine/PostgreSQL отдельно не сканировались. |

## Проверка зависимостей V1 (историческая)

Первый `govulncheck` завершился с ошибкой и сообщил о четырёх потенциально достижимых уязвимостях:
Go 1.27.3 (`GO-2026-6859`), pgx 5.7.6 (`GO-2026-5004`), x/text 0.24.0
(`GO-2026-5970`, `GO-2026-6629`). Это реальные результаты сканер, не
воспроизведённые атаки. Для pgx advisory требует simple protocol и специально
сформированный dollar-quoted SQL; API использует default pgx protocol и не строит
таких SQL из входа. Нельзя из этого автоматически заключить, что exploit доказан
в данном сервисе. Зависимость всё равно обновлена.

Финальные версии: Go **1.27.4** (go.mod и Dockerfile), pgx **5.12.0**, x/text
**0.41.0**. Повторный сканер: `No vulnerabilities found.` Проверки приложения
повторены на новой сборке. Сканирование локального source графа не заменяет scan
container OS, runtime testing и будущие обновления vulnerability database.

Источники: [pgx advisory](https://pkg.go.dev/vuln/GO-2026-5004),
[x/text advisory](https://pkg.go.dev/vuln/GO-2026-6629).
Страница Go `GO-2026-6859` через web-tool оказалась недоступна; информация о ней
взята из сохранённого фактического вывода `govulncheck`, без утверждения о ручной
проверке exploit. Обновление host-wide Go не выполнялось: go.mod использует
автоматический toolchain download, Docker закрепляет исправленную версию.

## Изменения V2

RabbitMQ Management API, Prometheus, Collector и Tempo доступны только внутри
Compose-сети. Grafana публикуется на loopback с паролем; локальный демонстрационный
пароль нужно заменить. VPS-конфигурация требует отдельные секреты RabbitMQ и Grafana.
Трассы не содержат payload и секретов; метрики не используют job_id как метку.
Tempo работает от root для доступа к тому: усиление контейнера остаётся задачей
перед production. Политики хранения outbox/DLQ и ограничения backlog ещё нет.

Сканирование V2 обнаружило пять достижимых уязвимостей x/net 0.59.0. После обновления
до 0.60.0 повторный govulncheck не нашёл достижимых вызовов, но сообщил о двух
уязвимостях модулей вне обнаруженного графа вызовов. Это не подтверждает отсутствие
всех уязвимостей. Исходные выводы сохранены в [результатах V2](results/v2/INDEX.md).
Образы ОС отдельно не сканировались. Vendor фиксирует исходники зависимостей,
но не освобождает от их обновления и повторной проверки.
