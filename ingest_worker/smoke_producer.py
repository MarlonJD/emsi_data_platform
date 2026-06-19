from __future__ import annotations

import json
import os
from datetime import datetime, timezone

from kafka import KafkaProducer

from ingest_worker.common import DEFAULT_EVENT_TOPIC, csv_env


DEFAULT_VALID_EVENT_ID = "local-dev-smoke-event-v1"
DEFAULT_INVALID_EVENT_ID = "local-dev-smoke-invalid-v1"


def main() -> int:
    topic = os.getenv("ANALYTICS_EVENT_TOPIC", DEFAULT_EVENT_TOPIC)
    producer = KafkaProducer(
        bootstrap_servers=csv_env("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092"),
        client_id="emsi-data-platform-smoke-producer",
    )

    valid = valid_event()
    invalid = invalid_event()
    producer.send(topic, key=encoded_key(valid["event_id"]), value=encoded_value(valid))
    producer.send(topic, key=encoded_key(invalid["event_id"]), value=encoded_value(invalid))
    producer.flush(timeout=30)
    producer.close()
    print(
        "published ingest smoke events: "
        f"valid_event_id={valid['event_id']} invalid_event_id={invalid['event_id']} topic={topic}"
    )
    return 0


def valid_event() -> dict[str, object]:
    now = utc_now()
    return {
        "event_id": os.getenv("SMOKE_EVENT_ID", DEFAULT_VALID_EVENT_ID),
        "event_name": "data_platform.local_smoke.accepted",
        "event_version": 1,
        "occurred_at": now,
        "received_at": now,
        "producer": "emsi-data-platform-smoke",
        "privacy_class": "pseudonymous",
        "consent_scope": ["analytics"],
        "subject": {
            "user_hash": "sha256:local-smoke-user",
            "session_id": "local-smoke-session",
        },
        "payload": {
            "surface": "local_smoke",
            "item_type": "event",
            "rank": 1,
        },
    }


def invalid_event() -> dict[str, object]:
    now = utc_now()
    return {
        "event_id": os.getenv("SMOKE_INVALID_EVENT_ID", DEFAULT_INVALID_EVENT_ID),
        "event_name": "data_platform.local_smoke.rejected",
        "event_version": 1,
        "occurred_at": now,
        "received_at": now,
        "producer": "emsi-data-platform-smoke",
        "privacy_class": "pseudonymous",
        "consent_scope": ["analytics"],
        "subject": {
            "user_hash": "sha256:local-smoke-user",
            "session_id": "local-smoke-session",
        },
        "payload": {
            "surface": "local_smoke",
            "user_id": "not-a-real-user",
        },
    }


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def encoded_key(value: object) -> bytes:
    return str(value).encode("utf-8")


def encoded_value(value: dict[str, object]) -> bytes:
    return json.dumps(value, separators=(",", ":")).encode("utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
