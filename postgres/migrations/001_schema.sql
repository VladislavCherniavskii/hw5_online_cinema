CREATE TABLE IF NOT EXISTS daily_metrics
(
    metric_date DATE NOT NULL,
    metric_name TEXT NOT NULL,
    metric_value DOUBLE PRECISION,
    computed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (metric_date, metric_name)
);

ALTER TABLE daily_metrics ALTER COLUMN metric_value DROP NOT NULL;

CREATE TABLE IF NOT EXISTS top_movies
(
    metric_date DATE NOT NULL,
    rank_num INTEGER NOT NULL,
    movie_id TEXT NOT NULL,
    views BIGINT NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (metric_date, rank_num)
);

CREATE TABLE IF NOT EXISTS retention_cohorts
(
    cohort_date DATE NOT NULL,
    day_number SMALLINT NOT NULL CHECK (day_number BETWEEN 0 AND 7),
    cohort_size BIGINT NOT NULL,
    retained_users BIGINT NOT NULL,
    retention_rate DOUBLE PRECISION NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (cohort_date, day_number)
);

CREATE TABLE IF NOT EXISTS device_distribution
(
    metric_date DATE NOT NULL,
    device_type TEXT NOT NULL,
    events BIGINT NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (metric_date, device_type)
);

CREATE INDEX IF NOT EXISTS idx_daily_metrics_date ON daily_metrics(metric_date);
CREATE INDEX IF NOT EXISTS idx_top_movies_date ON top_movies(metric_date);
