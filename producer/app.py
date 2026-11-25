import json
import logging
import os
import random
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Literal

import requests
from confluent_kafka import KafkaException, Producer
from fastapi import FastAPI, HTTPException
from fastavro import parse_schema, schemaless_writer
from fastavro.validation import validate
from pydantic import BaseModel, ConfigDict, Field, field_validator

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("movie-producer")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092,kafka2:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081").rstrip("/")
TOPIC = os.getenv("TOPIC", "movie-events")
GENERATOR_ENABLED = os.getenv("GENERATOR_ENABLED", "true").lower() == "true"
GENERATOR_INTERVAL_SECONDS = max(0.1, float(os.getenv("GENERATOR_INTERVAL_SECONDS", "1.0")))
GENERATOR_USERS = max(1, int(os.getenv("GENERATOR_USERS", "50")))
PUBLISH_ATTEMPTS = max(1, int(os.getenv("PUBLISH_ATTEMPTS", "6")))
PUBLISH_TIMEOUT_SECONDS = max(1.0, float(os.getenv("PUBLISH_TIMEOUT_SECONDS", "20")))

EVENT_TYPES = ("VIEW_STARTED", "VIEW_FINISHED", "VIEW_PAUSED", "VIEW_RESUMED", "LIKED", "SEARCHED")
DEVICE_TYPES = ("MOBILE", "DESKTOP", "TV", "TABLET")
SCHEMA_PATH = Path(__file__).parent / "schemas" / "movie-event-v1.avsc"
with SCHEMA_PATH.open("r", encoding="utf-8") as schema_file:
    AVRO_SCHEMA = json.load(schema_file)
PARSED_AVRO_SCHEMA = parse_schema(AVRO_SCHEMA)


class Event(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: uuid.UUID
    user_id: str = Field(min_length=1, max_length=128)
    movie_id: str = Field(min_length=1, max_length=128)
    event_type: Literal[
        "VIEW_STARTED", "VIEW_FINISHED", "VIEW_PAUSED", "VIEW_RESUMED", "LIKED", "SEARCHED"
    ]
    timestamp: datetime
    device_type: Literal["MOBILE", "DESKTOP", "TV", "TABLET"]
    session_id: str = Field(min_length=1, max_length=128)
    progress_seconds: int = Field(ge=0, le=2_147_483_647)

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @field_validator("user_id", "movie_id", "session_id")
    @classmethod
    def reject_blank_ids(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("must not be blank")
        return value


producer: Producer | None = None
schema_id: int | None = None
schema_version: int | None = None
generator_thread: threading.Thread | None = None
stop_generator = threading.Event()


def wait_for_schema_registry() -> None:
    while True:
        try:
            response = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects", timeout=5)
            response.raise_for_status()
            return
        except requests.RequestException as error:
            logger.warning("Schema Registry is not ready: %s", error)
            time.sleep(2)


def register_schema() -> tuple[int, int]:
    subject = f"{TOPIC}-value"
    response = requests.post(
        f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions",
        json={"schema": json.dumps(AVRO_SCHEMA, separators=(",", ":"))},
        timeout=15,
    )
    response.raise_for_status()
    registered_id = int(response.json()["id"])

    latest = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions/latest", timeout=10)
    latest.raise_for_status()
    return registered_id, int(latest.json()["version"])


def to_avro_record(event: Event) -> dict:
    return {
        "event_id": str(event.event_id),
        "user_id": event.user_id,
        "movie_id": event.movie_id,
        "event_type": event.event_type,
        "timestamp": event.timestamp,
        "device_type": event.device_type,
        "session_id": event.session_id,
        "progress_seconds": event.progress_seconds,
    }


def encode_event(event: Event) -> bytes:
    if schema_id is None:
        raise RuntimeError("Avro schema has not been registered")

    record = to_avro_record(event)
    validation_record = dict(record)
    validation_record["timestamp"] = int(event.timestamp.timestamp() * 1000)
    try:
        validate(validation_record, PARSED_AVRO_SCHEMA)
    except Exception as error:
        raise ValueError(f"Event does not match Avro schema: {error}") from error

    payload = BytesIO()
    payload.write(b"\x00")  # Confluent wire-format magic byte.
    payload.write(schema_id.to_bytes(4, byteorder="big", signed=False))
    schemaless_writer(payload, PARSED_AVRO_SCHEMA, record)
    return payload.getvalue()


def publish_event(event: Event) -> str:
    if producer is None:
        raise HTTPException(status_code=503, detail="Kafka producer is not ready")

    try:
        value = encode_event(event)
    except ValueError as error:
        raise HTTPException(status_code=422, detail=str(error)) from error

    last_error: Exception | None = None
    for attempt in range(1, PUBLISH_ATTEMPTS + 1):
        delivery_result: dict[str, Exception | None] = {"error": None}
        try:
            producer.produce(
                TOPIC,
                key=event.user_id.encode("utf-8"),
                value=value,
                on_delivery=lambda error, message: delivery_result.__setitem__("error", error),
            )
            remaining = producer.flush(PUBLISH_TIMEOUT_SECONDS)
            if remaining:
                raise TimeoutError(f"{remaining} Kafka message(s) were not delivered before timeout")
            if delivery_result["error"] is not None:
                raise KafkaException(delivery_result["error"])

            logger.info(
                "published event_id=%s event_type=%s timestamp=%s partition_key=user_id:%s",
                event.event_id,
                event.event_type,
                event.timestamp.isoformat(),
                event.user_id,
            )
            return str(event.event_id)
        except Exception as error:  # retried with bounded exponential backoff
            last_error = error
            logger.warning("Kafka publish attempt %d/%d failed: %s", attempt, PUBLISH_ATTEMPTS, error)
            if attempt < PUBLISH_ATTEMPTS:
                time.sleep(min(0.25 * (2 ** (attempt - 1)), 5.0))

    raise HTTPException(status_code=503, detail=f"Kafka publish failed: {last_error}")


def make_event(user_id: str, session_id: str, movie_id: str, device_type: str,
               event_type: str, progress_seconds: int = 0) -> Event:
    return Event(
        event_id=uuid.uuid4(),
        user_id=user_id,
        session_id=session_id,
        movie_id=movie_id,
        device_type=device_type,
        event_type=event_type,
        timestamp=datetime.now(timezone.utc),
        progress_seconds=progress_seconds,
    )


def generate_session(user_id: str, movies: list[str]) -> None:
    session_id = str(uuid.uuid4())
    movie_id = random.choice(movies)
    device_type = random.choice(DEVICE_TYPES)

    # Search precedes playback; playback events in this session are monotonic.
    publish_event(make_event(user_id, session_id, movie_id, device_type, "SEARCHED", 0))
    time.sleep(random.uniform(0.05, 0.2))
    publish_event(make_event(user_id, session_id, movie_id, device_type, "VIEW_STARTED", 0))

    progress = random.randint(20, 45)
    if random.random() < 0.35:
        time.sleep(random.uniform(0.05, 0.15))
        publish_event(make_event(user_id, session_id, movie_id, device_type, "VIEW_PAUSED", progress))
        progress += random.randint(5, 15)
        time.sleep(random.uniform(0.05, 0.15))
        publish_event(make_event(user_id, session_id, movie_id, device_type, "VIEW_RESUMED", progress))

    progress += random.randint(60, 240)
    time.sleep(random.uniform(0.05, 0.2))
    publish_event(make_event(user_id, session_id, movie_id, device_type, "VIEW_FINISHED", progress))
    if random.random() < 0.3:
        publish_event(make_event(user_id, session_id, movie_id, device_type, "LIKED", progress))


def generator_loop() -> None:
    users = [f"user_{index}" for index in range(GENERATOR_USERS)]
    movies = [f"movie_{index}" for index in range(20)]
    while not stop_generator.is_set():
        try:
            generate_session(random.choice(users), movies)
        except Exception:
            logger.exception("Synthetic event generation failed")
        stop_generator.wait(GENERATOR_INTERVAL_SECONDS * random.uniform(0.5, 1.5))


@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer, schema_id, schema_version, generator_thread

    wait_for_schema_registry()
    schema_id, schema_version = register_schema()
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "movie-service-producer",
        "acks": "all",
        "enable.idempotence": True,
        "retries": 10,
        "max.in.flight.requests.per.connection": 5,
        "message.timeout.ms": int(PUBLISH_TIMEOUT_SECONDS * 1000),
        "linger.ms": 10,
    })
    logger.info("Producer ready topic=%s schema_id=%s schema_version=%s", TOPIC, schema_id, schema_version)

    if GENERATOR_ENABLED:
        stop_generator.clear()
        generator_thread = threading.Thread(target=generator_loop, name="event-generator", daemon=True)
        generator_thread.start()

    try:
        yield
    finally:
        stop_generator.set()
        if generator_thread is not None:
            generator_thread.join(timeout=5)
        if producer is not None:
            producer.flush(PUBLISH_TIMEOUT_SECONDS)
        producer = None


app = FastAPI(title="Online Cinema Movie Service", version="1.0.0", lifespan=lifespan)


@app.get("/health")
def health() -> dict:
    if producer is None or schema_id is None:
        raise HTTPException(status_code=503, detail="Producer is not ready")
    try:
        metadata = producer.list_topics(timeout=2)
        if TOPIC not in metadata.topics:
            raise RuntimeError(f"Kafka topic {TOPIC} is unavailable")
        registry_response = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects", timeout=2)
        registry_response.raise_for_status()
    except Exception as error:
        logger.warning("Producer health check failed: %s", error)
        raise HTTPException(status_code=503, detail="Kafka or Schema Registry is unavailable") from error
    return {
        "status": "ok",
        "topic": TOPIC,
        "brokers": len(metadata.brokers),
        "schema_id": schema_id,
        "schema_version": schema_version,
    }


@app.get("/schema")
def schema_info() -> dict:
    return {"subject": f"{TOPIC}-value", "schema_id": schema_id, "version": schema_version,
            "schema": AVRO_SCHEMA}


@app.post("/events", status_code=200)
def create_event(event: Event) -> dict:
    return {"event_id": publish_event(event)}
