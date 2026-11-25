CREATE DATABASE IF NOT EXISTS default;

CREATE TABLE IF NOT EXISTS movie_events_kafka
(
    event_id String,
    user_id String,
    movie_id String,
    event_type Enum8('VIEW_STARTED' = 0, 'VIEW_FINISHED' = 1, 'VIEW_PAUSED' = 2, 'VIEW_RESUMED' = 3, 'LIKED' = 4, 'SEARCHED' = 5),
    timestamp DateTime64(3, 'UTC'),
    device_type Enum8('MOBILE' = 0, 'DESKTOP' = 1, 'TV' = 2, 'TABLET' = 3),
    session_id String,
    progress_seconds Int32
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka1:9092,kafka2:9092',
    kafka_topic_list = 'movie-events',
    kafka_group_name = 'clickhouse-movie-events-v1',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:8081',
    kafka_num_consumers = 3,
    kafka_thread_per_consumer = 1,
    kafka_skip_broken_messages = 0;

CREATE TABLE IF NOT EXISTS movie_events
(
    event_id String,
    user_id String,
    movie_id String,
    event_type LowCardinality(String),
    timestamp DateTime64(3, 'UTC'),
    device_type LowCardinality(String),
    session_id String,
    progress_seconds Int32,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (toDate(timestamp), user_id, timestamp, event_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS movie_events_mv
TO movie_events
AS
SELECT
    event_id,
    user_id,
    movie_id,
    toString(event_type) AS event_type,
    timestamp,
    toString(device_type) AS device_type,
    session_id,
    progress_seconds,
    now64(3) AS ingested_at
FROM movie_events_kafka;

CREATE TABLE IF NOT EXISTS daily_metrics
(
    metric_date Date,
    metric_name LowCardinality(String),
    metric_value Float64,
    computed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(metric_date)
ORDER BY (metric_date, metric_name);

CREATE TABLE IF NOT EXISTS top_movies
(
    metric_date Date,
    rank UInt32,
    movie_id String,
    views UInt64,
    computed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(metric_date)
ORDER BY (metric_date, rank);

CREATE TABLE IF NOT EXISTS retention_cohort
(
    cohort_date Date,
    day_number UInt8,
    cohort_size UInt64,
    retained_users UInt64,
    retention_rate Float64,
    computed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(cohort_date)
ORDER BY (cohort_date, day_number);

CREATE TABLE IF NOT EXISTS device_distribution
(
    metric_date Date,
    device_type LowCardinality(String),
    events UInt64,
    computed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(metric_date)
ORDER BY (metric_date, device_type);
