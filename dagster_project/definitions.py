from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Any

from dagster import AssetSelection, Definitions, asset, define_asset_job


WORKSPACE_DIR = Path(os.getenv("DATA_PLATFORM_WORKSPACE", "/workspace"))
DBT_PROJECT_DIR = Path(os.getenv("DBT_PROJECT_DIR", WORKSPACE_DIR / "dbt"))
SODA_CONFIG_PATH = Path(os.getenv("SODA_CONFIG_PATH", WORKSPACE_DIR / "soda" / "configuration.yml"))
SODA_CONTRACT_PATH = Path(
    os.getenv("SODA_CONTRACT_PATH", WORKSPACE_DIR / "soda" / "contracts" / "raw_event_landing.yml")
)

BLOCKED_IDENTIFIER_PATTERN = (
    r'("email"|"phone"|"authorization"|"token"|"user_id"|"userId"|'
    r'"raw_user_id"|"actor_user_id"|"actorUserId")'
)


@asset(group_name="platform_baseline")
def platform_baseline_decisions() -> dict[str, str]:
    return {
        "postgres": "baseline: postgres:18.4-alpine3.24",
        "python": "baseline: python:3.12.13-slim-bookworm",
        "dbt": "baseline: dbt-core==1.11.11 and dbt-postgres==1.10.1",
        "data_vault": "baseline: ScalefreeCOM/datavault4dbt==1.18.3",
        "soda": "local-dev: soda-core==4.14.0 and soda-postgres==4.14.0",
    }


@asset(group_name="phase_d_local_smoke")
def analytics_raw_event_landing_smoke(context) -> dict[str, int]:
    import psycopg2

    dsn = (
        f"host={os.getenv('ANALYTICS_POSTGRES_HOST', 'analytics-postgres')} "
        f"port={os.getenv('ANALYTICS_POSTGRES_PORT', '5432')} "
        f"dbname={os.getenv('ANALYTICS_POSTGRES_DB', 'analytics')} "
        f"user={os.getenv('ANALYTICS_POSTGRES_USER', 'analytics')} "
        f"password={os.getenv('ANALYTICS_POSTGRES_PASSWORD', 'analytics_local_password')}"
    )
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  count(*)::int AS row_count,
                  (count(*) - count(DISTINCT event_id))::int AS duplicate_event_id_count,
                  count(*) FILTER (WHERE event_id IS NULL)::int AS missing_event_id_count,
                  count(*) FILTER (
                    WHERE occurred_at IS NULL
                       OR received_at IS NULL
                       OR occurred_at > received_at + interval '5 minutes'
                       OR occurred_at < timestamp with time zone '2020-01-01T00:00:00Z'
                       OR received_at > now() + interval '5 minutes'
                  )::int AS invalid_timestamp_count,
                  count(*) FILTER (
                    WHERE privacy_class NOT IN ('pseudonymous')
                  )::int AS invalid_privacy_class_count,
                  count(*) FILTER (
                    WHERE subject_user_hash LIKE '%%@%%'
                       OR subject::text ~* %s
                       OR payload::text ~* %s
                  )::int AS raw_identifier_count
                FROM analytics.raw_event_landing
                """,
                (BLOCKED_IDENTIFIER_PATTERN, BLOCKED_IDENTIFIER_PATTERN),
            )
            columns = [column[0] for column in cur.description]
            metrics = dict(zip(columns, cur.fetchone(), strict=True))

    context.add_output_metadata(metrics)
    failures = {
        key: value
        for key, value in metrics.items()
        if (key == "row_count" and value <= 0) or (key != "row_count" and value != 0)
    }
    if failures:
        raise RuntimeError(f"raw_event_landing smoke failed: {failures}")
    return metrics


@asset(group_name="phase_d_local_smoke")
def dbt_phase_d_smoke(
    context,
    analytics_raw_event_landing_smoke: dict[str, int],
) -> dict[str, str]:
    _ = analytics_raw_event_landing_smoke
    commands = [
        ["dbt", "deps", "--project-dir", str(DBT_PROJECT_DIR)],
        ["dbt", "debug", "--project-dir", str(DBT_PROJECT_DIR), "--profiles-dir", str(DBT_PROJECT_DIR)],
        [
            "dbt",
            "run",
            "--project-dir",
            str(DBT_PROJECT_DIR),
            "--profiles-dir",
            str(DBT_PROJECT_DIR),
            "--select",
            "tag:phase_d_smoke",
        ],
    ]
    for command in commands:
        run_command(context, command)
    return {"status": "passed", "project_dir": str(DBT_PROJECT_DIR)}


@asset(group_name="phase_d_local_smoke")
def soda_raw_event_landing_scan(
    context,
    dbt_phase_d_smoke: dict[str, str],
) -> dict[str, str]:
    _ = dbt_phase_d_smoke
    run_command(
        context,
        [
            "soda",
            "contract",
            "verify",
            "--data-source",
            str(SODA_CONFIG_PATH),
            "--contract",
            str(SODA_CONTRACT_PATH),
        ],
    )
    return {"status": "passed", "contract_path": str(SODA_CONTRACT_PATH)}


def run_command(context: Any, command: list[str]) -> None:
    context.log.info("Running local smoke command: %s", " ".join(command))
    env = os.environ.copy()
    env.setdefault("DBT_PROFILES_DIR", str(DBT_PROJECT_DIR))
    completed = subprocess.run(
        command,
        cwd=str(WORKSPACE_DIR),
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    if completed.stdout:
        context.log.info(completed.stdout)
    if completed.returncode != 0:
        raise RuntimeError(
            f"local smoke command failed with exit {completed.returncode}: {' '.join(command)}"
        )


phase_d_local_smoke_job = define_asset_job(
    name="phase_d_local_smoke_job",
    selection=AssetSelection.groups("phase_d_local_smoke"),
)


defs = Definitions(
    assets=[
        platform_baseline_decisions,
        analytics_raw_event_landing_smoke,
        dbt_phase_d_smoke,
        soda_raw_event_landing_scan,
    ],
    jobs=[phase_d_local_smoke_job],
)
