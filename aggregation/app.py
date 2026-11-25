import json
import logging
import os
import time
import threading
from contextlib import asynccontextmanager
from datetime import date as Date, datetime, timedelta, timezone

import boto3
import psycopg2
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from botocore.config import Config
from fastapi import FastAPI, HTTPException

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("movie-aggregation")

CLICKHOUSE_URL = os.getenv("CLICKHOUSE_URL", "http://clickhouse:8123").rstrip("/")
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://movie:movie@postgres:5432/movie_analytics")
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "http://minio:9000")
S3_BUCKET = os.getenv("S3_BUCKET", "movie-analytics")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
INTERVAL_SECONDS = max(1, int(os.getenv("AGGREGATION_INTERVAL_SECONDS", "300")))
RETRY_ATTEMPTS = max(1, int(os.getenv("RETRY_ATTEMPTS", "5")))


def retry_delay(attempt: int) -> float:
    return min(0.5 * (2 ** (attempt - 1)), 10.0)


def ch_request(sql: str, body: bytes | None = None, timeout: int = 120) -> requests.Response:
    last_error: Exception | None = None
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            response = requests.post(
                CLICKHOUSE_URL,
                params={"query": sql} if body is not None else None,
                data=body if body is not None else sql.encode("utf-8"),
                timeout=timeout,
            )
            response.raise_for_status()
            return response
        except requests.RequestException as error:
            last_error = error
            logger.warning("ClickHouse attempt %d/%d failed: %s", attempt, RETRY_ATTEMPTS, error)
            if attempt < RETRY_ATTEMPTS:
                time.sleep(retry_delay(attempt))
    raise RuntimeError(f"ClickHouse request failed: {last_error}") from last_error


def ch_json(sql: str) -> list[dict]:
    response = ch_request(sql + " FORMAT JSON")
    return response.json()["data"]


def ch_insert(table: str, rows: list[dict]) -> None:
    if not rows:
        return
    body = "\n".join(json.dumps(row, ensure_ascii=False, default=str) for row in rows).encode("utf-8")
    ch_request(f"INSERT INTO {table} FORMAT JSONEachRow", body=body)


def ch_delete_for_date(table: str, date_column: str, day: str) -> None:
    # Tables are append-friendly ReplacingMergeTree tables. A synchronous mutation
    # makes a full recomputation replace, rather than append to, the previous result.
    ch_request(
        f"ALTER TABLE {table} DELETE WHERE {date_column} = toDate('{day}') SETTINGS mutations_sync = 1"
    )


def fetch_retention(metric_date: Date, offset: int, cohort_size: int) -> tuple[int, float]:
    day = metric_date.isoformat()
    query = f"""
        WITH first_views AS
        (
            SELECT user_id, min(toDate(timestamp)) AS first_date
            FROM movie_events FINAL
            WHERE event_type = 'VIEW_STARTED'
            GROUP BY user_id
        )
        SELECT uniqExact(user_id) AS retained_users
        FROM movie_events FINAL
        WHERE toDate(timestamp) = addDays(toDate('{day}'), {offset})
          AND user_id IN
          (
              SELECT user_id
              FROM first_views
              WHERE first_date = toDate('{day}')
          )
    """
    retained = int(ch_json(query)[0]["retained_users"])
    rate = (retained / cohort_size * 100.0) if cohort_size else 0.0
    return retained, rate


def write_postgres(metric_date: Date, metrics: list[dict], top_movies: list[dict],
                   retention_rows: list[dict], device_rows: list[dict], computed_at: datetime) -> None:
    last_error: Exception | None = None
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        connection = None
        try:
            connection = psycopg2.connect(POSTGRES_DSN, connect_timeout=10)
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute("DELETE FROM daily_metrics WHERE metric_date = %s", (metric_date,))
                    cursor.executemany(
                        "INSERT INTO daily_metrics (metric_date, metric_name, metric_value, computed_at) "
                        "VALUES (%s, %s, %s, %s)",
                        [(metric_date, item["metric_name"], item["metric_value"], computed_at)
                         for item in metrics],
                    )

                    cursor.execute("DELETE FROM top_movies WHERE metric_date = %s", (metric_date,))
                    cursor.executemany(
                        "INSERT INTO top_movies (metric_date, rank_num, movie_id, views, computed_at) "
                        "VALUES (%s, %s, %s, %s, %s)",
                        [(metric_date, item["rank"], item["movie_id"], item["views"], computed_at)
                         for item in top_movies],
                    )

                    cursor.execute("DELETE FROM retention_cohorts WHERE cohort_date = %s", (metric_date,))
                    cursor.executemany(
                        "INSERT INTO retention_cohorts "
                        "(cohort_date, day_number, cohort_size, retained_users, retention_rate, computed_at) "
                        "VALUES (%s, %s, %s, %s, %s, %s)",
                        [(metric_date, item["day_number"], item["cohort_size"], item["retained_users"],
                          item["retention_rate"], computed_at) for item in retention_rows],
                    )

                    cursor.execute("DELETE FROM device_distribution WHERE metric_date = %s", (metric_date,))
                    cursor.executemany(
                        "INSERT INTO device_distribution (metric_date, device_type, events, computed_at) "
                        "VALUES (%s, %s, %s, %s)",
                        [(metric_date, item["device_type"], item["events"], computed_at)
                         for item in device_rows],
                    )
            logger.info("PostgreSQL commit succeeded for %s on attempt %d", metric_date, attempt)
            return
        except Exception as error:
            last_error = error
            logger.exception("PostgreSQL write attempt %d/%d failed for %s", attempt, RETRY_ATTEMPTS, metric_date)
            if attempt < RETRY_ATTEMPTS:
                time.sleep(retry_delay(attempt))
        finally:
            if connection is not None:
                connection.close()
    raise RuntimeError(f"PostgreSQL write failed: {last_error}") from last_error


def _run_aggregation(metric_date: Date) -> dict:
    started = time.monotonic()
    day = metric_date.isoformat()
    computed_at = datetime.now(timezone.utc)
    ch_computed_at = computed_at.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    logger.info("Aggregation started date=%s", day)

    try:
        base = ch_json(f"""
            SELECT
                count() AS total_events,
                uniqExact(user_id) AS dau,
                if(countIf(event_type = 'VIEW_FINISHED') = 0, 0,
                   avgIf(progress_seconds, event_type = 'VIEW_FINISHED')) AS avg_watch_time,
                uniqExactIf(tuple(user_id, session_id), event_type = 'VIEW_STARTED') AS view_started,
                uniqExactIf(tuple(user_id, session_id), event_type = 'VIEW_FINISHED') AS view_finished
            FROM movie_events FINAL
            WHERE toDate(timestamp) = toDate('{day}')
        """)[0]
        total_events = int(base["total_events"])
        dau = int(base["dau"])
        avg_watch_time = float(base["avg_watch_time"])
        starts = int(base["view_started"])
        finishes = int(base["view_finished"])
        conversion = finishes / starts if starts else 0.0

        cohort_size = int(ch_json(f"""
            WITH first_views AS
            (
                SELECT user_id, min(toDate(timestamp)) AS first_date
                FROM movie_events FINAL
                WHERE event_type = 'VIEW_STARTED'
                GROUP BY user_id
            )
            SELECT count() AS cohort_size
            FROM first_views
            WHERE first_date = toDate('{day}')
        """)[0]["cohort_size"])

        # Retention is only reported once the corresponding day has elapsed.
        # For example, D7 is absent for a cohort younger than seven days.
        today_utc = datetime.now(timezone.utc).date()
        cohort_age_days = (today_utc - metric_date).days
        # Day N is complete only after its calendar day has ended. Day 0 is
        # available immediately because cohort membership itself guarantees activity.
        max_observed_day = max(0, min(7, cohort_age_days - 1))
        retention_rows: list[dict] = []
        retention_by_day: dict[int, tuple[int, float]] = {}
        if cohort_size > 0:
            for offset in range(max_observed_day + 1):
                retained, rate = fetch_retention(metric_date, offset, cohort_size)
                retention_by_day[offset] = (retained, rate)
                retention_rows.append({
                    "cohort_date": day,
                    "day_number": offset,
                    "cohort_size": cohort_size,
                    "retained_users": retained,
                    "retention_rate": rate,
                    "computed_at": ch_computed_at,
                })

        top_rows = ch_json(f"""
            SELECT movie_id, uniqExact(tuple(user_id, session_id)) AS views
            FROM movie_events FINAL
            WHERE toDate(timestamp) = toDate('{day}')
              AND event_type = 'VIEW_STARTED'
            GROUP BY movie_id
            ORDER BY views DESC, movie_id ASC
            LIMIT 10
        """)
        top_movies = [
            {"metric_date": day, "rank": rank, "movie_id": row["movie_id"],
             "views": int(row["views"]), "computed_at": ch_computed_at}
            for rank, row in enumerate(top_rows, start=1)
        ]

        device_data = ch_json(f"""
            SELECT device_type, count() AS events
            FROM movie_events FINAL
            WHERE toDate(timestamp) = toDate('{day}')
            GROUP BY device_type
            ORDER BY events DESC
        """)
        device_rows = [
            {"metric_date": day, "device_type": row["device_type"],
             "events": int(row["events"]), "computed_at": ch_computed_at}
            for row in device_data
        ]

        metrics = [
            {"metric_date": day, "metric_name": "total_events", "metric_value": total_events,
             "computed_at": ch_computed_at},
            {"metric_date": day, "metric_name": "dau", "metric_value": dau,
             "computed_at": ch_computed_at},
            {"metric_date": day, "metric_name": "avg_watch_time_seconds", "metric_value": avg_watch_time,
             "computed_at": ch_computed_at},
            {"metric_date": day, "metric_name": "view_started", "metric_value": starts,
             "computed_at": ch_computed_at},
            {"metric_date": day, "metric_name": "view_finished", "metric_value": finishes,
             "computed_at": ch_computed_at},
            {"metric_date": day, "metric_name": "conversion_rate", "metric_value": conversion,
             "computed_at": ch_computed_at},
            {"metric_date": day, "metric_name": "retention_cohort_size", "metric_value": cohort_size,
             "computed_at": ch_computed_at},
        ]
        if cohort_size > 0 and 1 in retention_by_day:
            metrics.append({"metric_date": day, "metric_name": "retention_d1",
                            "metric_value": retention_by_day[1][1] / 100.0, "computed_at": ch_computed_at})
        if cohort_size > 0 and 7 in retention_by_day:
            metrics.append({"metric_date": day, "metric_name": "retention_d7",
                            "metric_value": retention_by_day[7][1] / 100.0, "computed_at": ch_computed_at})
        for row in device_rows:
            metrics.append({"metric_date": day, "metric_name": f"device_{row['device_type'].lower()}_events",
                            "metric_value": row["events"], "computed_at": ch_computed_at})

        # Replace all previous materialized aggregates for this date.
        ch_delete_for_date("daily_metrics", "metric_date", day)
        ch_delete_for_date("top_movies", "metric_date", day)
        ch_delete_for_date("retention_cohort", "cohort_date", day)
        ch_delete_for_date("device_distribution", "metric_date", day)
        ch_insert("daily_metrics", metrics)
        ch_insert("top_movies", top_movies)
        ch_insert("retention_cohort", retention_rows)
        ch_insert("device_distribution", device_rows)

        write_postgres(metric_date, metrics, top_movies, retention_rows, device_rows, computed_at)
        duration = time.monotonic() - started
        logger.info(
            "Aggregation finished date=%s events=%d metric_rows=%d top_movies=%d retention_rows=%d "
            "device_rows=%d duration_seconds=%.3f",
            day, total_events, len(metrics), len(top_movies), len(retention_rows), len(device_rows), duration,
        )
        return {
            "date": day,
            "events_processed": total_events,
            "dau": dau,
            "metric_rows": len(metrics),
            "top_movies": len(top_movies),
            "retention_rows": len(retention_rows),
            "device_rows": len(device_rows),
            "duration_seconds": round(duration, 3),
        }
    except Exception:
        logger.exception("Aggregation failed date=%s duration_seconds=%.3f", day, time.monotonic() - started)
        raise


aggregation_lock = threading.Lock()


def run_aggregation(metric_date: Date) -> dict:
    # A scheduler run and an HTTP-triggered recomputation must not mutate the
    # same aggregate tables concurrently.
    with aggregation_lock:
        return _run_aggregation(metric_date)


def export_date(metric_date: Date) -> dict:
    day = metric_date.isoformat()
    logger.info("S3 export started date=%s", day)

    records: dict[str, list[dict]] | None = None
    last_error: Exception | None = None
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            connection = psycopg2.connect(POSTGRES_DSN, connect_timeout=10)
            try:
                with connection.cursor() as cursor:
                    cursor.execute(
                        "SELECT metric_name, metric_value, computed_at FROM daily_metrics "
                        "WHERE metric_date = %s ORDER BY metric_name", (metric_date,),
                    )
                    metrics = [
                        {"metric_name": row[0], "metric_value": row[1], "computed_at": row[2].isoformat()}
                        for row in cursor.fetchall()
                    ]
                    cursor.execute(
                        "SELECT rank_num, movie_id, views, computed_at FROM top_movies "
                        "WHERE metric_date = %s ORDER BY rank_num", (metric_date,),
                    )
                    top_movies = [
                        {"rank": row[0], "movie_id": row[1], "views": row[2], "computed_at": row[3].isoformat()}
                        for row in cursor.fetchall()
                    ]
                    cursor.execute(
                        "SELECT day_number, cohort_size, retained_users, retention_rate, computed_at "
                        "FROM retention_cohorts WHERE cohort_date = %s ORDER BY day_number", (metric_date,),
                    )
                    retention = [
                        {"day_number": row[0], "cohort_size": row[1], "retained_users": row[2],
                         "retention_rate_percent": row[3], "computed_at": row[4].isoformat()}
                        for row in cursor.fetchall()
                    ]
                    cursor.execute(
                        "SELECT device_type, events, computed_at FROM device_distribution "
                        "WHERE metric_date = %s ORDER BY device_type", (metric_date,),
                    )
                    devices = [
                        {"device_type": row[0], "events": row[1], "computed_at": row[2].isoformat()}
                        for row in cursor.fetchall()
                    ]
            finally:
                connection.close()
            if not metrics:
                raise ValueError(f"No aggregates found for {day}; call /recompute first")
            records = {"metrics": metrics, "top_movies": top_movies,
                       "retention_cohorts": retention, "device_distribution": devices}
            break
        except Exception as error:
            last_error = error
            logger.exception("PostgreSQL export read attempt %d/%d failed for %s", attempt, RETRY_ATTEMPTS, day)
            if attempt < RETRY_ATTEMPTS:
                time.sleep(retry_delay(attempt))
    if records is None:
        raise RuntimeError(f"Could not read aggregates from PostgreSQL: {last_error}") from last_error

    payload = json.dumps(
        {"date": day, "exported_at": datetime.now(timezone.utc).isoformat(), **records},
        ensure_ascii=False,
        indent=2,
    ).encode("utf-8")
    object_key = f"daily/{day}/aggregates.json"
    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )

    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=object_key,
                Body=payload,
                ContentType="application/json; charset=utf-8",
            )
            logger.info("S3 export finished s3://%s/%s", S3_BUCKET, object_key)
            return {
                "date": day,
                "bucket": S3_BUCKET,
                "key": object_key,
                "metrics": len(records["metrics"]),
                "top_movies": len(records["top_movies"]),
                "retention_rows": len(records["retention_cohorts"]),
                "device_rows": len(records["device_distribution"]),
            }
        except Exception as error:
            logger.exception("S3 upload attempt %d/%d failed for %s", attempt, RETRY_ATTEMPTS, day)
            if attempt == RETRY_ATTEMPTS:
                raise RuntimeError(f"S3 export failed: {error}") from error
            time.sleep(retry_delay(attempt))
    raise RuntimeError("S3 export failed")


scheduler = BackgroundScheduler(timezone="UTC")
last_cohort_refresh_date: Date | None = None


def scheduled_job() -> None:
    global last_cohort_refresh_date

    today_utc = datetime.now(timezone.utc).date()
    started = time.monotonic()
    logger.info("Scheduled aggregation cycle started current_date=%s", today_utc)
    failures = 0
    try:
        # Keep the current-day metrics fresh for Grafana on every interval.
        run_aggregation(today_utc)
    except Exception:
        failures += 1
        logger.exception("Scheduled current-day aggregation failed for %s", today_utc)

    # D1..D7 become complete as UTC calendar days end. Refresh the rolling
    # eight cohorts once after each date change, not every five-minute cycle.
    if last_cohort_refresh_date != today_utc:
        completed_dates = [today_utc - timedelta(days=offset) for offset in range(1, 9)]
        logger.info("Refreshing completed cohorts dates=%s",
                    [day.isoformat() for day in completed_dates])
        cohort_failures = 0
        for target_date in completed_dates:
            try:
                run_aggregation(target_date)
                export_date(target_date)
            except Exception:
                cohort_failures += 1
                logger.exception("Scheduled aggregation/export failed for %s", target_date)
        failures += cohort_failures
        if cohort_failures == 0:
            last_cohort_refresh_date = today_utc

    logger.info(
        "Scheduled aggregation cycle finished failures=%d duration_seconds=%.3f",
        failures, time.monotonic() - started,
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler.add_job(
        scheduled_job,
        trigger="interval",
        seconds=INTERVAL_SECONDS,
        id="daily-aggregation-export",
        max_instances=1,
        coalesce=True,
        next_run_time=datetime.now(timezone.utc) + timedelta(seconds=20),
    )
    scheduler.start()
    logger.info("Aggregation scheduler started interval_seconds=%d", INTERVAL_SECONDS)
    try:
        yield
    finally:
        scheduler.shutdown(wait=False)


app = FastAPI(title="Online Cinema Aggregation Service", version="1.0.0", lifespan=lifespan)


@app.get("/health")
def health() -> dict:
    connection = None
    try:
        response = requests.post(
            CLICKHOUSE_URL,
            data=b"SELECT 1 AS ready FORMAT JSON",
            timeout=2,
        )
        response.raise_for_status()
        if response.json()["data"][0]["ready"] != 1:
            raise RuntimeError("ClickHouse readiness query returned an unexpected result")
        connection = psycopg2.connect(POSTGRES_DSN, connect_timeout=2)
    except Exception as error:
        logger.warning("Aggregation health check failed: %s", error)
        raise HTTPException(status_code=503, detail="ClickHouse or PostgreSQL is unavailable") from error
    finally:
        if connection is not None:
            connection.close()
    return {"status": "ok", "interval_seconds": INTERVAL_SECONDS}


def parse_date(value: str) -> Date:
    try:
        return Date.fromisoformat(value)
    except ValueError as error:
        raise HTTPException(status_code=400, detail="date must be YYYY-MM-DD") from error


@app.post("/recompute")
def recompute(date: str) -> dict:
    return run_aggregation(parse_date(date))


@app.post("/export")
def export(date: str) -> dict:
    return export_date(parse_date(date))


@app.post("/run")
def run(date: str) -> dict:
    target_date = parse_date(date)
    aggregation_result = run_aggregation(target_date)
    export_result = export_date(target_date)
    return {"aggregation": aggregation_result, "export": export_result}
