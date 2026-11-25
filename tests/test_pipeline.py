import os
import time
import uuid
from datetime import datetime, timezone

import requests

PRODUCER_URL = os.getenv("PRODUCER_URL", "http://producer:8000").rstrip("/")
CLICKHOUSE_URL = os.getenv("CLICKHOUSE_URL", "http://clickhouse:8123").rstrip("/")
PIPELINE_TIMEOUT_SECONDS = int(os.getenv("PIPELINE_TIMEOUT_SECONDS", "90"))


def clickhouse_query(sql: str) -> list[dict]:
    response = requests.post(CLICKHOUSE_URL, params={"query": sql + " FORMAT JSON"}, timeout=15)
    response.raise_for_status()
    return response.json()["data"]


def test_invalid_event_is_rejected() -> None:
    payload = {
        "event_id": str(uuid.uuid4()),
        "user_id": "test-user",
        "movie_id": "movie-test",
        "event_type": "NOT_AN_EVENT",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "device_type": "DESKTOP",
        "session_id": str(uuid.uuid4()),
        "progress_seconds": 0,
    }
    response = requests.post(f"{PRODUCER_URL}/events", json=payload, timeout=15)
    assert response.status_code == 422, response.text


def test_event_pipeline_kafka_to_clickhouse() -> None:
    event_id = str(uuid.uuid4())
    user_id = f"integration-test-{uuid.uuid4()}"
    session_id = str(uuid.uuid4())
    timestamp = datetime.now(timezone.utc)
    timestamp = timestamp.replace(microsecond=(timestamp.microsecond // 1000) * 1000)
    payload = {
        "event_id": event_id,
        "user_id": user_id,
        "movie_id": "movie-integration-test",
        "event_type": "VIEW_STARTED",
        "timestamp": timestamp.isoformat(),
        "device_type": "DESKTOP",
        "session_id": session_id,
        "progress_seconds": 0,
    }

    response = requests.post(f"{PRODUCER_URL}/events", json=payload, timeout=30)
    assert response.status_code == 200, response.text
    assert response.json()["event_id"] == event_id

    deadline = time.monotonic() + PIPELINE_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        rows = clickhouse_query(
            "SELECT event_id, user_id, movie_id, event_type, toUnixTimestamp64Milli(timestamp) AS timestamp_ms, "
            "device_type, session_id, progress_seconds "
            f"FROM movie_events FINAL WHERE event_id = '{event_id}'"
        )
        if rows:
            row = rows[0]
            assert row["event_id"] == event_id
            assert row["user_id"] == user_id
            assert row["movie_id"] == "movie-integration-test"
            assert row["event_type"] == "VIEW_STARTED"
            assert int(row["timestamp_ms"]) == int(timestamp.timestamp() * 1000)
            assert row["device_type"] == "DESKTOP"
            assert row["session_id"] == session_id
            assert int(row["progress_seconds"]) == 0
            return
        time.sleep(1)

    raise AssertionError(f"event {event_id} was not found in ClickHouse within {PIPELINE_TIMEOUT_SECONDS}s")
