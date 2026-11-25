# HW5 — Online Cinema: Event Streaming + Analytics Pipeline

###

- Avro-схема `producer/schemas/movie-event-v1.avsc`; время описано как Avro `timestamp-millis`, идентификатор события проверяется приложением как UUID.
- Producer регистрирует схему в Schema Registry под subject `movie-events-value`; версия и schema id доступны в `GET /schema` и `GET /health`.
- Kafka topic `movie-events`: 3 партиции, replication factor 2, `min.insync.replicas=1`.
- Ключ Kafka-сообщения — `user_id`, поэтому события одного пользователя попадают в одну партицию и сохраняют порядок внутри неё.
- HTTP producer: `POST /events`, Avro validation, `acks=all`, idempotent producer, retry с exponential backoff и логирование публикаций.
- Встроенный генератор создаёт согласованные сессии: `SEARCHED → VIEW_STARTED → [VIEW_PAUSED → VIEW_RESUMED] → VIEW_FINISHED → [LIKED]`. Прогресс просмотра не уменьшается.
- ClickHouse Kafka Engine читает AvroConfluent из Kafka; входные `Enum8` и `DateTime64(3, 'UTC')` соответствуют Avro enum и `timestamp-millis`, а Materialized View переносит данные в `ReplacingMergeTree`.
- Интеграционные тесты проверяют отказ валидации и путь события Producer → Kafka → ClickHouse. Для каждого тестового события создаётся уникальный `event_id`.

###

- Отдельный сервис агрегации читает raw-события непосредственно из ClickHouse.
- Метрики: `total_events`, DAU, средний прогресс завершённого просмотра, количество стартов/завершений, conversion, retention D1/D7, размер когорты, топ фильмов и распределение по устройствам.
- ClickHouse-агрегаты сохраняются в отдельных таблицах `daily_metrics`, `top_movies`, `retention_cohort`, `device_distribution`.
- PostgreSQL хранит агрегаты, топ фильмов, retention cohort rows и device distribution. Повторный расчёт за дату заменяет прошлые строки в транзакции.
- Сервис запускает периодический расчёт и экспорт; интервал задаётся `AGGREGATION_INTERVAL_SECONDS`. Есть ручные эндпоинты `/recompute`, `/export` и `/run`.
- Экспорт JSON содержит все метрики, топ фильмов, когорты, устройства, дату и время выгрузки. Ключ объекта: `daily/YYYY-MM-DD/aggregates.json`; повторная выгрузка перезаписывает его.

###

- Два Kafka broker, Schema Registry и replication factor 2.
- Health checks для всех долгоживущих сервисов.
- Grafana автоматически устанавливает совместимую версию ClickHouse datasource, поднимает datasource и dashboard из provisioning-файлов.
- Dashboard содержит Retention Cohort Heatmap и дополнительные панели DAU, conversion, топа фильмов, устройств и среднего прогресса просмотра.
- PostgreSQL и ClickHouse migrations запускаются отдельными one-shot сервисами при `docker compose up`.


Требуются Docker Engine и Docker Compose v2. На macOS сначала запустите Docker Desktop. Если используете Colima:

```bash
colima start --cpu 4 --memory 6 --disk 40
docker context use colima
docker info
```

Если `docker info` не показывает Server, проблема в неработающем Docker Engine, а не в приложении.

Затем из корня проекта:

```bash
docker compose config -q
docker compose up --build -d
```

`.env.example` можно скопировать в `.env` только при необходимости изменить значения генератора или интервал агрегации; без `.env` используются значения по умолчанию.

Проверить состояние контейнеров:

```bash
docker compose ps
```

Проверить готовность сервисов:

```bash
curl http://localhost:8000/health
curl http://localhost:8001/health
curl http://localhost:8082/subjects
curl 'http://localhost:8123/?query=SELECT%201'
```

На первом запуске Confluent Kafka-образы могут стартовать дольше остальных. Продюсер запускается после создания топика, а агрегация — после миграций ClickHouse/PostgreSQL и создания MinIO bucket.


После того как все долгоживущие сервисы стали `healthy`, а миграции завершились с кодом 0:

```bash
docker compose ps -a
docker compose --profile test run --build --rm tests
```

Тестовые события изолированы UUID и не требуют предварительной очистки таблиц. Повторный запуск теста допустим.


Swagger producer: <http://localhost:8000/docs>

Swagger aggregation: <http://localhost:8001/docs>

Пример отправки события:

```bash
curl -X POST 'http://localhost:8000/events' \
  -H 'Content-Type: application/json' \
  -d '{
    "event_id": "11111111-1111-4111-8111-111111111111",
    "user_id": "user-demo",
    "movie_id": "movie-42",
    "event_type": "VIEW_STARTED",
    "timestamp": "2026-11-25T12:00:00Z",
    "device_type": "DESKTOP",
    "session_id": "session-demo",
    "progress_seconds": 0
  }'
```

Ручной пересчёт и экспорт за текущую UTC-дату:

```bash
DATE=$(date -u +%F)
curl -X POST "http://localhost:8001/recompute?date=${DATE}"
curl -X POST "http://localhost:8001/export?date=${DATE}"
```

Один вызов для пересчёта и выгрузки:

```bash
curl -X POST "http://localhost:8001/run?date=$(date -u +%F)"
```


| Компонент | Адрес | Доступ |
|---|---|---|
| Producer API | http://localhost:8000/docs | — |
| Aggregation API | http://localhost:8001/docs | — |
| Schema Registry | http://localhost:8082 | — |
| ClickHouse HTTP | http://localhost:8123 | user `default`, без пароля (локальная разработка) |
| PostgreSQL | localhost:5433 | `movie` / `movie`, database `movie_analytics` |
| Grafana | http://localhost:3000 | `admin` / `admin` |
| MinIO Console | http://localhost:9004 | `minioadmin` / `minioadmin` |
| MinIO S3 API | http://localhost:9003 | `minioadmin` / `minioadmin` |

Bucket `movie-analytics` создаётся автоматически.


- **DAU** — `uniqExact(user_id)` среди всех событий за календарный день UTC.
- **Среднее время просмотра** — `avg(progress_seconds)` среди `VIEW_FINISHED` за день. Это среднее итогового прогресса, а не длительность wall-clock сессии.
- **Топ фильмов** — число уникальных пар `(user_id, session_id)` с `VIEW_STARTED` по `movie_id` за день.
- **Conversion** — число уникальных пользовательских сессий с `VIEW_FINISHED` / число уникальных сессий с `VIEW_STARTED` за день; значение в таблице метрик хранится как доля от 0 до 1.
- **Retention D1/D7** — доля пользователей, чей первый `VIEW_STARTED` пришёлся на дату когорты и у которых было хотя бы одно событие в точности через 1/7 календарных дней. D1/D7 не материализуется до наступления соответствующего дня, чтобы не показывать ложный ноль для незрелой когорты.
- **Cohort Heatmap** — строки по дате первого `VIEW_STARTED`, столбцы Day 0–Day 7, значения в процентах от 0 до 100. Для ещё не наступивших дней ячейки пустые.

Все даты агрегирования рассчитываются в UTC.


`AGGREGATION_INTERVAL_SECONDS` по умолчанию равен 300 секунд. Каждый цикл обновляет агрегаты за текущую дату для Grafana. Один раз при смене UTC-даты сервис пересчитывает и экспортирует восемь завершённых дат (вчерашнюю и семь предшествующих когорт). Это нужно, чтобы Day 1–Day 7 попадали в retention только после полного завершения соответствующего календарного дня, но не выполнять тяжёлые когортные запросы каждые пять минут. При ошибке обновление повторяется на следующем цикле. Экспорт повторно записывает тот же S3-ключ, поэтому обновление retention не создаёт дубликаты. Повторный расчёт безопасен и заменяет результаты за указанную дату.


Остановить контейнеры, сохранив данные:

```bash
docker compose down
```

Полностью очистить данные Kafka/ClickHouse/PostgreSQL/MinIO/Grafana и начать с нуля:

```bash
docker compose down -v
```

