from __future__ import annotations

import base64
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable

import psycopg2

from ingest_worker.common import int_env, postgres_config


EVENT_TABLE = "analytics_events_local_candidate"
AGGREGATE_TABLE = "analytics_event_hourly_counts_local_candidate"
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class ClickHouseConfig:
    host: str
    port: int
    database: str
    user: str
    password: str
    timeout_seconds: float

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}/"


def main() -> int:
    clickhouse = clickhouse_config()
    database = clickhouse_identifier(clickhouse.database)
    limit = int_env("CLICKHOUSE_EXPORT_LIMIT", 500)
    if limit <= 0:
        print("clickhouse candidate smoke failed: CLICKHOUSE_EXPORT_LIMIT must be positive", file=sys.stderr)
        return 1

    rows = read_landing_rows(limit)
    if not rows:
        print(
            "clickhouse candidate smoke failed: analytics.raw_event_landing is empty; "
            "run ./scripts/run_ingest_smoke.sh first",
            file=sys.stderr,
        )
        return 1

    try:
        ensure_clickhouse_schema(clickhouse, database)
        load_clickhouse_candidate(clickhouse, database, rows)

        event_ids = [str(row["event_id"]) for row in rows]
        postgres_start = time.perf_counter()
        postgres_aggregate = read_postgres_hourly_aggregate(event_ids)
        postgres_ms = elapsed_ms(postgres_start)

        clickhouse_start = time.perf_counter()
        clickhouse_aggregate = read_clickhouse_hourly_aggregate(clickhouse, database)
        clickhouse_ms = elapsed_ms(clickhouse_start)
    except (urllib.error.URLError, RuntimeError, psycopg2.Error) as exc:
        print(f"clickhouse candidate smoke failed: {exc}", file=sys.stderr)
        return 1

    if postgres_aggregate != clickhouse_aggregate:
        print(
            "clickhouse candidate smoke failed: aggregate mismatch\n"
            f"postgres={json.dumps(postgres_aggregate, sort_keys=True)}\n"
            f"clickhouse={json.dumps(clickhouse_aggregate, sort_keys=True)}",
            file=sys.stderr,
        )
        return 1

    event_count = sum(row[3] for row in clickhouse_aggregate)
    print(
        "clickhouse candidate smoke passed: "
        f"rows={len(rows)} aggregate_events={event_count} "
        f"postgres_aggregate_ms={postgres_ms:.2f} clickhouse_aggregate_ms={clickhouse_ms:.2f} "
        "benchmark_scope=local_candidate_bounded"
    )
    return 0


def clickhouse_config() -> ClickHouseConfig:
    return ClickHouseConfig(
        host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
        port=int_env("CLICKHOUSE_PORT", 8123),
        database=os.getenv("CLICKHOUSE_DB", "emsi_hot_analytics"),
        user=os.getenv("CLICKHOUSE_USER", "emsi_hot_analytics"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "clickhouse_local_password"),
        timeout_seconds=float(os.getenv("CLICKHOUSE_TIMEOUT_SECONDS", "30")),
    )


def clickhouse_identifier(value: str) -> str:
    if not IDENTIFIER_RE.match(value):
        raise RuntimeError(f"unsafe ClickHouse identifier: {value!r}")
    return f"`{value}`"


def read_landing_rows(limit: int) -> list[dict[str, object]]:
    cfg = postgres_config()
    with psycopg2.connect(cfg.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  event_id,
                  event_name,
                  event_version,
                  occurred_at,
                  received_at,
                  producer,
                  privacy_class,
                  subject_user_hash,
                  payload_sha256,
                  raw_record_sha256,
                  raw_record_bytes,
                  source_topic,
                  source_partition,
                  source_offset,
                  landed_at
                FROM analytics.raw_event_landing
                ORDER BY landed_at DESC, event_id DESC
                LIMIT %s
                """,
                (limit,),
            )
            columns = [desc[0] for desc in cur.description]
            rows = [dict(zip(columns, row, strict=True)) for row in cur.fetchall()]
    rows.reverse()
    return rows


def read_postgres_hourly_aggregate(event_ids: list[str]) -> list[tuple[str, str, str, int]]:
    cfg = postgres_config()
    with psycopg2.connect(cfg.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  to_char(date_trunc('hour', occurred_at AT TIME ZONE 'UTC'), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS event_hour,
                  event_name,
                  privacy_class,
                  count(*)::integer AS event_count
                FROM analytics.raw_event_landing
                WHERE event_id = ANY(%s)
                GROUP BY 1, 2, 3
                ORDER BY 1, 2, 3
                """,
                (event_ids,),
            )
            return [(row[0], row[1], row[2], row[3]) for row in cur.fetchall()]


def ensure_clickhouse_schema(config: ClickHouseConfig, database: str) -> None:
    query_clickhouse(config, f"CREATE DATABASE IF NOT EXISTS {database}")
    query_clickhouse(
        config,
        f"""
        CREATE TABLE IF NOT EXISTS {database}.{EVENT_TABLE}
        (
          event_id String,
          event_name LowCardinality(String),
          event_version UInt16,
          occurred_at DateTime64(3, 'UTC'),
          received_at DateTime64(3, 'UTC'),
          producer LowCardinality(String),
          privacy_class LowCardinality(String),
          subject_user_hash String,
          payload_sha256 String,
          raw_record_sha256 String,
          raw_record_bytes UInt32,
          source_topic String,
          source_partition Int32,
          source_offset Int64,
          landed_at DateTime64(3, 'UTC'),
          ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMM(occurred_at)
        ORDER BY (event_name, occurred_at, event_id)
        """,
    )
    query_clickhouse(
        config,
        f"""
        CREATE TABLE IF NOT EXISTS {database}.{AGGREGATE_TABLE}
        (
          event_hour DateTime('UTC'),
          event_name LowCardinality(String),
          privacy_class LowCardinality(String),
          event_count UInt64
        )
        ENGINE = SummingMergeTree
        PARTITION BY toYYYYMM(event_hour)
        ORDER BY (event_hour, event_name, privacy_class)
        """,
    )


def load_clickhouse_candidate(
    config: ClickHouseConfig,
    database: str,
    rows: Iterable[dict[str, object]],
) -> None:
    query_clickhouse(config, f"TRUNCATE TABLE {database}.{EVENT_TABLE}")
    query_clickhouse(config, f"TRUNCATE TABLE {database}.{AGGREGATE_TABLE}")

    payload = "\n".join(json.dumps(clickhouse_event_row(row), sort_keys=True) for row in rows).encode("utf-8")
    query_clickhouse(
        config,
        f"INSERT INTO {database}.{EVENT_TABLE} FORMAT JSONEachRow",
        payload,
    )
    query_clickhouse(
        config,
        f"""
        INSERT INTO {database}.{AGGREGATE_TABLE}
        SELECT
          toStartOfHour(occurred_at) AS event_hour,
          event_name,
          privacy_class,
          count() AS event_count
        FROM {database}.{EVENT_TABLE}
        GROUP BY event_hour, event_name, privacy_class
        """,
    )


def clickhouse_event_row(row: dict[str, object]) -> dict[str, object]:
    return {
        "event_id": row["event_id"],
        "event_name": row["event_name"],
        "event_version": row["event_version"],
        "occurred_at": clickhouse_datetime(row["occurred_at"]),
        "received_at": clickhouse_datetime(row["received_at"]),
        "producer": row["producer"],
        "privacy_class": row["privacy_class"],
        "subject_user_hash": row["subject_user_hash"],
        "payload_sha256": row["payload_sha256"],
        "raw_record_sha256": row["raw_record_sha256"],
        "raw_record_bytes": row["raw_record_bytes"],
        "source_topic": row["source_topic"],
        "source_partition": row["source_partition"],
        "source_offset": row["source_offset"],
        "landed_at": clickhouse_datetime(row["landed_at"]),
    }


def read_clickhouse_hourly_aggregate(
    config: ClickHouseConfig,
    database: str,
) -> list[tuple[str, str, str, int]]:
    result = query_clickhouse(
        config,
        f"""
        SELECT
          formatDateTime(event_hour, '%Y-%m-%dT%H:%i:%SZ') AS event_hour,
          event_name,
          privacy_class,
          sum(event_count) AS event_count
        FROM {database}.{AGGREGATE_TABLE}
        GROUP BY event_hour, event_name, privacy_class
        ORDER BY event_hour, event_name, privacy_class
        FORMAT TabSeparated
        """,
    )
    rows: list[tuple[str, str, str, int]] = []
    for line in result.splitlines():
        if not line:
            continue
        event_hour, event_name, privacy_class, event_count = line.split("\t")
        rows.append((event_hour, event_name, privacy_class, int(event_count)))
    return rows


def query_clickhouse(config: ClickHouseConfig, query: str, data: bytes | None = None) -> str:
    body = query.strip().encode("utf-8")
    if data is not None:
        body += b"\n" + data

    last_error: urllib.error.URLError | None = None
    for attempt in range(1, 21):
        request = urllib.request.Request(
            config.base_url,
            data=body,
            method="POST",
        )
        request.add_header("Authorization", basic_auth(config.user, config.password))
        request.add_header("Content-Type", "text/plain; charset=utf-8")

        try:
            with urllib.request.urlopen(request, timeout=config.timeout_seconds) as response:
                return response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"ClickHouse HTTP {exc.code}: {detail}") from exc
        except urllib.error.URLError as exc:
            last_error = exc
            if attempt == 20:
                break
            time.sleep(1)

    raise RuntimeError(f"ClickHouse HTTP connection failed after retries: {last_error}") from last_error


def basic_auth(user: str, password: str) -> str:
    token = base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii")
    return f"Basic {token}"


def clickhouse_datetime(value: object) -> str:
    if not isinstance(value, datetime):
        raise RuntimeError(f"expected datetime from Postgres, got {type(value).__name__}")
    dt = value
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:23]


def elapsed_ms(start: float) -> float:
    return (time.perf_counter() - start) * 1000


if __name__ == "__main__":
    raise SystemExit(main())
