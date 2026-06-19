from __future__ import annotations

import hashlib
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg2
from psycopg2.extras import Json
from kafka import KafkaConsumer, KafkaProducer, TopicPartition

from ingest_worker.common import (
    DEFAULT_CONSUMER_GROUP,
    DEFAULT_DLQ_TOPIC,
    DEFAULT_EVENT_TOPIC,
    csv_env,
    int_env,
    postgres_config,
)


BLOCKED_IDENTIFIER_KEYS = {
    "userid",
    "rawuserid",
    "actoruserid",
    "email",
    "phone",
    "authorization",
    "token",
}


class ValidationError(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class RecordLocation:
    topic: str
    partition: int
    offset: int


@dataclass(frozen=True)
class ValidatedEvent:
    event_id: str
    event_name: str
    event_version: int
    occurred_at: datetime
    received_at: datetime
    producer: str
    privacy_class: str
    consent_scope: list[str]
    subject: dict[str, Any]
    subject_user_hash: str
    subject_session_id: str | None
    payload: dict[str, Any]


def main() -> int:
    cfg = RuntimeConfig.from_env()
    conn = connect_with_retry()
    ensure_schema(conn, cfg.schema_sql_path)

    consumer = KafkaConsumer(
        cfg.event_topic,
        bootstrap_servers=cfg.bootstrap_servers,
        group_id=cfg.consumer_group,
        enable_auto_commit=False,
        auto_offset_reset=cfg.auto_offset_reset,
        client_id="emsi-data-platform-ingest-worker",
        value_deserializer=None,
        key_deserializer=None,
    )
    dlq_producer = KafkaProducer(
        bootstrap_servers=cfg.bootstrap_servers,
        client_id="emsi-data-platform-ingest-dlq",
    )

    print(
        "ingest worker started: "
        f"topic={cfg.event_topic} group={cfg.consumer_group} "
        f"bootstrap_servers={','.join(cfg.bootstrap_servers)}"
    )

    processed = 0
    idle_started_at: float | None = None
    try:
        while True:
            records = consumer.poll(timeout_ms=1000, max_records=cfg.max_poll_records)
            if not records:
                if cfg.idle_timeout_seconds > 0:
                    idle_started_at = idle_started_at or time.monotonic()
                    if time.monotonic() - idle_started_at >= cfg.idle_timeout_seconds:
                        break
                continue
            idle_started_at = None
            for topic_partition, messages in records.items():
                for message in messages:
                    lag = consumer_lag(consumer, topic_partition, message.offset)
                    process_message(conn, dlq_producer, cfg, message, lag)
                    consumer.commit()
                    processed += 1
                    if cfg.max_messages > 0 and processed >= cfg.max_messages:
                        return 0
    finally:
        dlq_producer.flush(timeout=10)
        dlq_producer.close()
        consumer.close()
        conn.close()
    print(f"ingest worker stopped after idle timeout: processed={processed}")
    return 0


@dataclass(frozen=True)
class RuntimeConfig:
    bootstrap_servers: list[str]
    event_topic: str
    dlq_topic: str
    consumer_group: str
    auto_offset_reset: str
    max_messages: int
    max_poll_records: int
    idle_timeout_seconds: int
    schema_sql_path: Path

    @classmethod
    def from_env(cls) -> "RuntimeConfig":
        return cls(
            bootstrap_servers=csv_env("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092"),
            event_topic=os.getenv("ANALYTICS_EVENT_TOPIC", DEFAULT_EVENT_TOPIC),
            dlq_topic=os.getenv("ANALYTICS_EVENT_DLQ_TOPIC", DEFAULT_DLQ_TOPIC),
            consumer_group=os.getenv("INGEST_CONSUMER_GROUP", DEFAULT_CONSUMER_GROUP),
            auto_offset_reset=os.getenv("INGEST_AUTO_OFFSET_RESET", "earliest"),
            max_messages=int_env("INGEST_MAX_MESSAGES", 0),
            max_poll_records=int_env("INGEST_MAX_POLL_RECORDS", 50),
            idle_timeout_seconds=int_env("INGEST_IDLE_TIMEOUT_SECONDS", 0),
            schema_sql_path=Path(
                os.getenv(
                    "INGEST_SCHEMA_SQL_PATH",
                    "sql/analytics-postgres-init/010_event_ingest.sql",
                )
            ),
        )


def connect_with_retry():
    cfg = postgres_config()
    attempts = int_env("ANALYTICS_POSTGRES_CONNECT_ATTEMPTS", 30)
    for attempt in range(1, attempts + 1):
        try:
            return psycopg2.connect(cfg.dsn)
        except psycopg2.OperationalError:
            if attempt == attempts:
                raise
            time.sleep(1)
    raise RuntimeError("unreachable")


def ensure_schema(conn, schema_sql_path: Path) -> None:
    with schema_sql_path.open("r", encoding="utf-8") as schema_file:
        schema_sql = schema_file.read()
    with conn:
        with conn.cursor() as cur:
            cur.execute(schema_sql)


def process_message(conn, dlq_producer, cfg: RuntimeConfig, message, lag: int | None) -> None:
    location = RecordLocation(message.topic, message.partition, message.offset)
    raw_value = bytes(message.value or b"")
    raw_hash = sha256_hex(raw_value)
    raw_size = len(raw_value)
    try:
        event = validate_event(raw_value)
    except ValidationError as err:
        event_id, event_name = safe_event_identity(raw_value)
        record_dlq(
            conn,
            cfg.consumer_group,
            location,
            event_id,
            event_name,
            err.code,
            err.message,
            raw_hash,
            raw_size,
            lag,
        )
        publish_dlq_metadata(
            dlq_producer,
            cfg.dlq_topic,
            location,
            event_id,
            event_name,
            err,
            raw_hash,
            raw_size,
        )
        print(
            "ingest rejected event: "
            f"topic={location.topic} partition={location.partition} offset={location.offset} "
            f"error_code={err.code}"
        )
        return

    inserted = record_landing(
        conn,
        cfg.consumer_group,
        location,
        event,
        raw_hash,
        raw_size,
        lag,
    )
    status = "accepted" if inserted else "duplicate"
    print(
        f"ingest {status} event: event_id={event.event_id} "
        f"topic={location.topic} partition={location.partition} offset={location.offset}"
    )


def validate_event(raw_value: bytes) -> ValidatedEvent:
    if not raw_value:
        raise ValidationError("empty_value", "event value is empty")
    try:
        decoded = json.loads(raw_value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as err:
        raise ValidationError("invalid_json", "event value must be a JSON object") from err
    if not isinstance(decoded, dict):
        raise ValidationError("invalid_json_object", "event value must be a JSON object")

    event_id = required_string(decoded, "event_id")
    event_name = required_string(decoded, "event_name")
    event_version = decoded.get("event_version")
    if not isinstance(event_version, int) or event_version <= 0:
        raise ValidationError("invalid_event_version", "event_version must be a positive integer")
    occurred_at = parse_timestamp(decoded.get("occurred_at"), "occurred_at")
    received_at = parse_timestamp(decoded.get("received_at"), "received_at")
    producer = required_string(decoded, "producer")
    privacy_class = required_string(decoded, "privacy_class")
    if privacy_class != "pseudonymous":
        raise ValidationError("invalid_privacy_class", "privacy_class must be pseudonymous")
    consent_scope = decoded.get("consent_scope")
    if not isinstance(consent_scope, list) or not all(isinstance(value, str) for value in consent_scope):
        raise ValidationError("invalid_consent_scope", "consent_scope must be a string array")
    if "analytics" not in {value.strip().lower() for value in consent_scope}:
        raise ValidationError("missing_analytics_consent", "analytics consent scope is required")
    subject = decoded.get("subject")
    if not isinstance(subject, dict):
        raise ValidationError("invalid_subject", "subject must be an object")
    user_hash = subject.get("user_hash")
    if not isinstance(user_hash, str) or not user_hash.strip():
        raise ValidationError("missing_subject_hash", "subject.user_hash is required")
    if "@" in user_hash:
        raise ValidationError("raw_subject_hash", "subject.user_hash must not contain contact data")
    session_id = subject.get("session_id")
    if session_id is not None and not isinstance(session_id, str):
        raise ValidationError("invalid_session_id", "subject.session_id must be a string")
    payload = decoded.get("payload")
    if not isinstance(payload, dict):
        raise ValidationError("invalid_payload", "payload must be a JSON object")
    if contains_blocked_identifier_key(payload):
        raise ValidationError("blocked_payload_identifier", "payload contains a blocked raw identifier key")

    return ValidatedEvent(
        event_id=event_id,
        event_name=event_name,
        event_version=event_version,
        occurred_at=occurred_at,
        received_at=received_at,
        producer=producer,
        privacy_class=privacy_class,
        consent_scope=consent_scope,
        subject=subject,
        subject_user_hash=user_hash,
        subject_session_id=session_id,
        payload=payload,
    )


def record_landing(
    conn,
    consumer_group: str,
    location: RecordLocation,
    event: ValidatedEvent,
    raw_hash: str,
    raw_size: int,
    lag: int | None,
) -> bool:
    payload_hash = sha256_hex(canonical_json(event.payload))
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO analytics.raw_event_landing (
                  event_id, event_name, event_version, occurred_at, received_at,
                  producer, privacy_class, consent_scope, subject, subject_user_hash,
                  subject_session_id, payload, source_topic, source_partition,
                  source_offset, raw_record_sha256, raw_record_bytes, payload_sha256
                )
                VALUES (
                  %s, %s, %s, %s, %s,
                  %s, %s, %s, %s, %s,
                  %s, %s, %s, %s,
                  %s, %s, %s, %s
                )
                ON CONFLICT (event_id) DO NOTHING
                """,
                (
                    event.event_id,
                    event.event_name,
                    event.event_version,
                    event.occurred_at,
                    event.received_at,
                    event.producer,
                    event.privacy_class,
                    Json(event.consent_scope),
                    Json(event.subject),
                    event.subject_user_hash,
                    event.subject_session_id,
                    Json(event.payload),
                    location.topic,
                    location.partition,
                    location.offset,
                    raw_hash,
                    raw_size,
                    payload_hash,
                ),
            )
            inserted = cur.rowcount == 1
            upsert_checkpoint(cur, consumer_group, location)
            upsert_metrics(cur, consumer_group, location.topic, location, accepted=1, rejected=0, lag=lag)
            return inserted


def record_dlq(
    conn,
    consumer_group: str,
    location: RecordLocation,
    event_id: str | None,
    event_name: str | None,
    error_code: str,
    error_message: str,
    raw_hash: str,
    raw_size: int,
    lag: int | None,
) -> None:
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO analytics.raw_event_dlq (
                  event_id, event_name, source_topic, source_partition, source_offset,
                  error_code, error_message, raw_record_sha256, raw_record_bytes
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    event_id,
                    event_name,
                    location.topic,
                    location.partition,
                    location.offset,
                    error_code,
                    error_message[:500],
                    raw_hash,
                    raw_size,
                ),
            )
            upsert_checkpoint(cur, consumer_group, location)
            upsert_metrics(cur, consumer_group, location.topic, location, accepted=0, rejected=1, lag=lag)


def publish_dlq_metadata(
    dlq_producer,
    dlq_topic: str,
    location: RecordLocation,
    event_id: str | None,
    event_name: str | None,
    err: ValidationError,
    raw_hash: str,
    raw_size: int,
) -> None:
    value = {
        "source_topic": location.topic,
        "source_partition": location.partition,
        "source_offset": location.offset,
        "event_id": event_id,
        "event_name": event_name,
        "error_code": err.code,
        "error_message": err.message[:500],
        "raw_record_sha256": raw_hash,
        "raw_record_bytes": raw_size,
        "created_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    }
    dlq_producer.send(
        dlq_topic,
        key=(event_id or raw_hash).encode("utf-8"),
        value=json.dumps(value, separators=(",", ":")).encode("utf-8"),
    )


def upsert_checkpoint(cur, consumer_group: str, location: RecordLocation) -> None:
    cur.execute(
        """
        INSERT INTO analytics.event_ingest_checkpoints (
          consumer_group, source_topic, source_partition, last_offset, updated_at
        )
        VALUES (%s, %s, %s, %s, now())
        ON CONFLICT (consumer_group, source_topic, source_partition)
        DO UPDATE SET
          last_offset = GREATEST(analytics.event_ingest_checkpoints.last_offset, EXCLUDED.last_offset),
          updated_at = now()
        """,
        (consumer_group, location.topic, location.partition, location.offset),
    )


def upsert_metrics(
    cur,
    consumer_group: str,
    source_topic: str,
    location: RecordLocation,
    accepted: int,
    rejected: int,
    lag: int | None,
) -> None:
    cur.execute(
        """
        INSERT INTO analytics.event_ingest_metrics (
          consumer_group, source_topic, accepted_count, rejected_count,
          last_processed_topic, last_processed_partition, last_processed_offset,
          last_lag, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())
        ON CONFLICT (consumer_group, source_topic)
        DO UPDATE SET
          accepted_count = analytics.event_ingest_metrics.accepted_count + EXCLUDED.accepted_count,
          rejected_count = analytics.event_ingest_metrics.rejected_count + EXCLUDED.rejected_count,
          last_processed_topic = EXCLUDED.last_processed_topic,
          last_processed_partition = EXCLUDED.last_processed_partition,
          last_processed_offset = EXCLUDED.last_processed_offset,
          last_lag = EXCLUDED.last_lag,
          updated_at = now()
        """,
        (
            consumer_group,
            source_topic,
            accepted,
            rejected,
            location.topic,
            location.partition,
            location.offset,
            lag,
        ),
    )


def consumer_lag(consumer, topic_partition, offset: int) -> int | None:
    try:
        end_offsets = consumer.end_offsets([topic_partition], timeout_ms=1000)
    except TypeError:
        try:
            end_offsets = consumer.end_offsets([topic_partition])
        except Exception:
            return None
    except Exception:
        return None
    end_offset = end_offsets.get(TopicPartition(topic_partition.topic, topic_partition.partition))
    if end_offset is None:
        return None
    return max(0, end_offset - offset - 1)


def required_string(decoded: dict[str, Any], key: str) -> str:
    value = decoded.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"missing_{key}", f"{key} is required")
    return value


def parse_timestamp(value: Any, field: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"missing_{field}", f"{field} is required")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as err:
        raise ValidationError(f"invalid_{field}", f"{field} must be an ISO-8601 timestamp") from err
    if parsed.tzinfo is None:
        raise ValidationError(f"invalid_{field}", f"{field} must include a timezone")
    return parsed.astimezone(timezone.utc)


def contains_blocked_identifier_key(value: Any) -> bool:
    if isinstance(value, dict):
        for key, nested in value.items():
            if normalize_key(str(key)) in BLOCKED_IDENTIFIER_KEYS:
                return True
            if contains_blocked_identifier_key(nested):
                return True
    elif isinstance(value, list):
        return any(contains_blocked_identifier_key(item) for item in value)
    return False


def normalize_key(key: str) -> str:
    return "".join(ch for ch in key.strip().lower() if ch not in {"_", "-", " "})


def safe_event_identity(raw_value: bytes) -> tuple[str | None, str | None]:
    try:
        decoded = json.loads(raw_value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None, None
    if not isinstance(decoded, dict):
        return None, None
    return safe_string(decoded.get("event_id")), safe_string(decoded.get("event_name"))


def safe_string(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    trimmed = value.strip()
    if not trimmed or "@" in trimmed or len(trimmed) > 200:
        return None
    return trimmed


def canonical_json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def sha256_hex(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("ingest worker interrupted", file=sys.stderr)
        raise SystemExit(130)
