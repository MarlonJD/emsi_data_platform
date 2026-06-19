from __future__ import annotations

import os
from dataclasses import dataclass


DEFAULT_EVENT_TOPIC = "emsi.analytics.events.v1"
DEFAULT_DLQ_TOPIC = "emsi.analytics.events.dlq.v1"
DEFAULT_CONSUMER_GROUP = "emsi-data-platform-ingest-local"


@dataclass(frozen=True)
class PostgresConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str

    @property
    def dsn(self) -> str:
        return (
            f"host={self.host} port={self.port} dbname={self.dbname} "
            f"user={self.user} password={self.password}"
        )


def csv_env(key: str, fallback: str) -> list[str]:
    value = os.getenv(key, fallback)
    return [part.strip() for part in value.split(",") if part.strip()]


def int_env(key: str, fallback: int) -> int:
    value = os.getenv(key)
    if not value:
        return fallback
    try:
        parsed = int(value)
    except ValueError:
        return fallback
    return parsed if parsed >= 0 else fallback


def postgres_config() -> PostgresConfig:
    return PostgresConfig(
        host=os.getenv("ANALYTICS_POSTGRES_HOST", "analytics-postgres"),
        port=int_env("ANALYTICS_POSTGRES_PORT", 5432),
        dbname=os.getenv("ANALYTICS_POSTGRES_DB", "analytics"),
        user=os.getenv("ANALYTICS_POSTGRES_USER", "analytics"),
        password=os.getenv("ANALYTICS_POSTGRES_PASSWORD", "analytics_local_password"),
    )
