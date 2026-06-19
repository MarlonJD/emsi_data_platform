from __future__ import annotations

import os
import sys
import time

import psycopg2

from ingest_worker.common import postgres_config


DEFAULT_VALID_EVENT_ID = "local-dev-smoke-event-v1"
DEFAULT_INVALID_EVENT_ID = "local-dev-smoke-invalid-v1"


def main() -> int:
    valid_event_id = os.getenv("SMOKE_EVENT_ID", DEFAULT_VALID_EVENT_ID)
    invalid_event_id = os.getenv("SMOKE_INVALID_EVENT_ID", DEFAULT_INVALID_EVENT_ID)
    attempts = int(os.getenv("SMOKE_CHECK_ATTEMPTS", "90"))
    sleep_seconds = float(os.getenv("SMOKE_CHECK_SLEEP_SECONDS", "1"))

    last = None
    for _ in range(attempts):
        last = read_counts(valid_event_id, invalid_event_id)
        if last["landing_count"] == 1 and last["dlq_count"] >= 1:
            print(
                "ingest smoke passed: "
                f"landing_count={last['landing_count']} dlq_count={last['dlq_count']} "
                f"checkpoint_count={last['checkpoint_count']}"
            )
            return 0
        time.sleep(sleep_seconds)

    print(
        "ingest smoke failed: "
        f"valid_event_id={valid_event_id} invalid_event_id={invalid_event_id} "
        f"last_counts={last}",
        file=sys.stderr,
    )
    return 1


def read_counts(valid_event_id: str, invalid_event_id: str) -> dict[str, int]:
    cfg = postgres_config()
    with psycopg2.connect(cfg.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM analytics.raw_event_landing WHERE event_id = %s",
                (valid_event_id,),
            )
            landing_count = cur.fetchone()[0]
            cur.execute(
                "SELECT count(*) FROM analytics.raw_event_dlq WHERE event_id = %s",
                (invalid_event_id,),
            )
            dlq_count = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM analytics.event_ingest_checkpoints")
            checkpoint_count = cur.fetchone()[0]
    return {
        "landing_count": landing_count,
        "dlq_count": dlq_count,
        "checkpoint_count": checkpoint_count,
    }


if __name__ == "__main__":
    raise SystemExit(main())
