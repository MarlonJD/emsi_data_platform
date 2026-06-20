from __future__ import annotations

import argparse
import json
import os
import re
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


APPROVAL_ID = "EMSI-DP-P1A-IOS-CANARY-20260620-AR01"
EXIT_BLOCKED = 2
EXIT_FAILED = 3

ALLOWED_EVENT_NAMES = {
    "app_session_started",
    "app_session_ended",
    "screen_viewed",
    "feed_item_impression",
    "feed_item_viewable_impression",
    "feed_item_open",
    "feed_item_like",
    "feed_item_reply",
    "feed_item_join",
    "feed_item_share",
    "feed_item_copy_link",
    "feed_item_hide",
    "feed_item_show_less",
    "feed_item_not_interested",
    "feed_item_mute_author",
    "feed_item_mute_channel",
}

SAFE_INTERACTION_EVENT_NAMES = {
    "feed_item_like",
    "feed_item_reply",
    "feed_item_join",
    "feed_item_share",
    "feed_item_copy_link",
    "feed_item_hide",
    "feed_item_show_less",
    "feed_item_not_interested",
    "feed_item_mute_author",
    "feed_item_mute_channel",
}

FORBIDDEN_KEYS = {
    "actor_user_id",
    "authorization",
    "auth_token",
    "bearer",
    "contact_payload",
    "email",
    "free_form",
    "freeform",
    "note",
    "phone",
    "raw_content",
    "raw_user_id",
    "support_contact",
    "token",
    "user_id",
    "userid",
}

LOCAL_DSN_MARKERS = (
    "localhost",
    "127.0.0.1",
    "::1",
    "host=postgres",
    "host=analytics-postgres",
    "sslmode=disable",
    "analytics_local_password",
    "postgres://emsi:emsi@",
)

CHECK_STATUSES = {"passed", "failed", "blocked", "skipped"}
SAFE_REF_PATTERN = re.compile(r"^[A-Za-z0-9._:-]+$")
HASH_PATTERN = re.compile(r"^(?:sha256:)?[a-fA-F0-9]{64}$")


@dataclass(frozen=True)
class CanaryConfig:
    target_name: str
    target_class: str
    seeded_user_ref: str
    subject_user_hash: str
    event_id_prefix: str
    window_start: datetime
    window_end: datetime
    warehouse_dsn: str
    share_analytics: bool
    personalization_enabled: bool
    privacy_artifact: str
    dbt_status: str
    dbt_artifact: str
    soda_status: str
    soda_artifact: str
    dagster_status: str
    dagster_artifact: str
    stop_rollback_outcome: str

    @property
    def duration_seconds(self) -> int:
        return int((self.window_end - self.window_start).total_seconds())


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Fail-closed capture helper for the EMSI Data Platform iOS limited "
            "production canary. This helper does not emit app events."
        )
    )
    parser.add_argument(
        "--preflight-only",
        action="store_true",
        help="Validate target/access inputs without connecting to the warehouse.",
    )
    parser.add_argument(
        "--evidence-json",
        type=Path,
        help="Write the bounded evidence summary JSON to this path.",
    )
    args = parser.parse_args()

    exit_code, evidence = collect_evidence(preflight_only=args.preflight_only)
    payload = json.dumps(evidence, indent=2, sort_keys=True)
    if args.evidence_json:
        args.evidence_json.parent.mkdir(parents=True, exist_ok=True)
        args.evidence_json.write_text(payload + "\n", encoding="utf-8")
        print(f"wrote canary evidence summary: {args.evidence_json}")
    else:
        print(payload)
    return exit_code


def collect_evidence(preflight_only: bool) -> tuple[int, dict[str, Any]]:
    cfg, errors, warnings = load_config(preflight_only)
    evidence = base_evidence(cfg, preflight_only, errors, warnings)

    if errors:
        evidence["classification"] = "blocked-access-gate"
        evidence["preflight"]["passed"] = False
        return EXIT_BLOCKED, evidence

    evidence["preflight"]["passed"] = True
    if preflight_only:
        evidence["classification"] = "preflight-ready"
        return 0, evidence

    try:
        landing_rows, dlq_count = read_canary_rows(cfg)
    except Exception as exc:  # pragma: no cover - exercised by runtime only.
        evidence["classification"] = "blocked-warehouse-check"
        evidence["preflight"]["passed"] = False
        evidence["preflight"]["errors"].append(
            f"warehouse capture failed without exposing credentials: {type(exc).__name__}"
        )
        return EXIT_BLOCKED, evidence

    counts = Counter(row["event_name"] for row in landing_rows)
    forbidden_hits = sum(count_forbidden_keys(row["subject"]) + count_forbidden_keys(row["payload"]) for row in landing_rows)
    out_of_scope_names = sorted(name for name in counts if name not in ALLOWED_EVENT_NAMES)
    criteria = success_criteria(counts, dlq_count, forbidden_hits, out_of_scope_names, cfg)

    evidence.update(
        {
            "classification": "production-limited-canary-capture",
            "landing_count": len(landing_rows),
            "dlq_count": dlq_count,
            "accepted_event_names": dict(sorted(counts.items())),
            "forbidden_field_result": {
                "forbidden_field_count": forbidden_hits,
                "out_of_scope_event_names": out_of_scope_names,
            },
            "downstream_checks": {
                "dbt": {"status": cfg.dbt_status, "artifact": cfg.dbt_artifact},
                "soda": {"status": cfg.soda_status, "artifact": cfg.soda_artifact},
                "dagster": {"status": cfg.dagster_status, "artifact": cfg.dagster_artifact},
            },
            "success_criteria": criteria,
            "stop_rollback_outcome": cfg.stop_rollback_outcome,
        }
    )

    if all(criteria.values()):
        evidence["result"] = "passed"
        return 0, evidence
    evidence["result"] = "failed"
    return EXIT_FAILED, evidence


def load_config(preflight_only: bool) -> tuple[CanaryConfig, list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []

    approval_id = env("EMSI_DP_CANARY_APPROVAL_ID")
    if approval_id != APPROVAL_ID:
        errors.append(f"EMSI_DP_CANARY_APPROVAL_ID must equal {APPROVAL_ID}")

    target_name = env("EMSI_DP_CANARY_TARGET_NAME")
    validate_safe_ref("EMSI_DP_CANARY_TARGET_NAME", target_name, errors)
    if any(
        marker in target_name.lower()
        for marker in ("local", "localhost", "dev", "development", "test")
    ):
        errors.append("EMSI_DP_CANARY_TARGET_NAME must not be local/dev/test")

    target_class = env("EMSI_DP_CANARY_TARGET_CLASS")
    if target_class not in {"production", "staging-production-equivalent"}:
        errors.append(
            "EMSI_DP_CANARY_TARGET_CLASS must be production or staging-production-equivalent"
        )

    seeded_user_ref = env("EMSI_DP_CANARY_SEEDED_USER_REF")
    validate_safe_ref("EMSI_DP_CANARY_SEEDED_USER_REF", seeded_user_ref, errors)
    subject_user_hash = env("EMSI_DP_CANARY_SUBJECT_USER_HASH")
    if not HASH_PATTERN.fullmatch(subject_user_hash):
        errors.append(
            "EMSI_DP_CANARY_SUBJECT_USER_HASH must be a pseudonymous 64-hex hash"
        )

    event_id_prefix = env("EMSI_DP_CANARY_EVENT_ID_PREFIX")
    validate_safe_ref("EMSI_DP_CANARY_EVENT_ID_PREFIX", event_id_prefix, errors)
    if len(event_id_prefix) < 16:
        errors.append("EMSI_DP_CANARY_EVENT_ID_PREFIX must be at least 16 characters")
    if "local" in event_id_prefix.lower() or "dev" in event_id_prefix.lower():
        errors.append("EMSI_DP_CANARY_EVENT_ID_PREFIX must not identify local/dev evidence")

    window_start = parse_time_env("EMSI_DP_CANARY_WINDOW_START", errors)
    window_end = parse_time_env("EMSI_DP_CANARY_WINDOW_END", errors)
    if window_start and window_end:
        duration = (window_end - window_start).total_seconds()
        if duration < 60 or duration > 120:
            errors.append("canary window must be between 60 and 120 seconds")
    else:
        now = datetime.now(timezone.utc).replace(microsecond=0)
        window_start = window_start or now
        window_end = window_end or now

    warehouse_dsn = env("EMSI_DP_CANARY_WAREHOUSE_DSN")
    validate_warehouse_dsn(warehouse_dsn, errors)

    share_analytics = bool_env("EMSI_DP_CANARY_SHARE_ANALYTICS", errors)
    personalization_enabled = bool_env("EMSI_DP_CANARY_PERSONALIZATION_ENABLED", errors)
    privacy_artifact = env("EMSI_DP_CANARY_PRIVACY_ARTIFACT")
    if not privacy_artifact:
        errors.append("EMSI_DP_CANARY_PRIVACY_ARTIFACT is required")
    if share_analytics is not True:
        errors.append("EMSI_DP_CANARY_SHARE_ANALYTICS must be true")
    if personalization_enabled is not True:
        errors.append("EMSI_DP_CANARY_PERSONALIZATION_ENABLED must be true")

    dbt_status = env("EMSI_DP_CANARY_DBT_STATUS")
    dbt_artifact = env("EMSI_DP_CANARY_DBT_ARTIFACT")
    soda_status = env("EMSI_DP_CANARY_SODA_STATUS")
    soda_artifact = env("EMSI_DP_CANARY_SODA_ARTIFACT")
    dagster_status = env("EMSI_DP_CANARY_DAGSTER_STATUS")
    dagster_artifact = env("EMSI_DP_CANARY_DAGSTER_ARTIFACT")
    stop_rollback_outcome = env("EMSI_DP_CANARY_STOP_ROLLBACK_OUTCOME")

    if preflight_only:
        for key in (
            "EMSI_DP_CANARY_DBT_STATUS",
            "EMSI_DP_CANARY_SODA_STATUS",
            "EMSI_DP_CANARY_DAGSTER_STATUS",
            "EMSI_DP_CANARY_STOP_ROLLBACK_OUTCOME",
        ):
            if not env(key):
                warnings.append(f"{key} is not required for preflight but is required for capture")
    else:
        if env("EMSI_DP_CANARY_ALLOW_PRODUCTION_CAPTURE") != "true":
            errors.append("EMSI_DP_CANARY_ALLOW_PRODUCTION_CAPTURE=true is required for capture")
        require_check_result("dbt", dbt_status, dbt_artifact, errors)
        require_check_result("Soda", soda_status, soda_artifact, errors)
        require_check_result("Dagster", dagster_status, dagster_artifact, errors)
        if not stop_rollback_outcome:
            errors.append("EMSI_DP_CANARY_STOP_ROLLBACK_OUTCOME is required for capture")

    return (
        CanaryConfig(
            target_name=target_name,
            target_class=target_class,
            seeded_user_ref=seeded_user_ref,
            subject_user_hash=subject_user_hash,
            event_id_prefix=event_id_prefix,
            window_start=window_start,
            window_end=window_end,
            warehouse_dsn=warehouse_dsn,
            share_analytics=share_analytics is True,
            personalization_enabled=personalization_enabled is True,
            privacy_artifact=privacy_artifact,
            dbt_status=dbt_status,
            dbt_artifact=dbt_artifact,
            soda_status=soda_status,
            soda_artifact=soda_artifact,
            dagster_status=dagster_status,
            dagster_artifact=dagster_artifact,
            stop_rollback_outcome=stop_rollback_outcome,
        ),
        errors,
        warnings,
    )


def base_evidence(
    cfg: CanaryConfig,
    preflight_only: bool,
    errors: list[str],
    warnings: list[str],
) -> dict[str, Any]:
    return {
        "approval_id": APPROVAL_ID,
        "classification": "pending",
        "target": {
            "name": cfg.target_name or None,
            "class": cfg.target_class or None,
        },
        "scope": {
            "platform": "ios",
            "seeded_user_only": True,
            "allowed_event_names": sorted(ALLOWED_EVENT_NAMES),
            "local_dev_evidence_allowed": False,
        },
        "seeded_user_ref": cfg.seeded_user_ref or None,
        "event_id_prefix": cfg.event_id_prefix or None,
        "window": {
            "start": isoformat_z(cfg.window_start),
            "end": isoformat_z(cfg.window_end),
            "duration_seconds": cfg.duration_seconds,
        },
        "privacy": {
            "shareAnalytics": cfg.share_analytics,
            "personalizationEnabled": cfg.personalization_enabled,
            "artifact": cfg.privacy_artifact or None,
        },
        "preflight": {
            "mode": "preflight-only" if preflight_only else "capture",
            "passed": False,
            "errors": errors,
            "warnings": warnings,
        },
        "landing_count": None,
        "dlq_count": None,
        "accepted_event_names": {},
        "forbidden_field_result": None,
        "downstream_checks": None,
        "success_criteria": None,
        "stop_rollback_outcome": None,
        "redacted": {
            "warehouse_dsn": bool(cfg.warehouse_dsn),
            "subject_user_hash": bool(cfg.subject_user_hash),
        },
    }


def read_canary_rows(cfg: CanaryConfig) -> tuple[list[dict[str, Any]], int]:
    try:
        import psycopg2
    except ImportError as exc:  # pragma: no cover - depends on runtime image.
        raise RuntimeError("psycopg2 is required for warehouse capture") from exc

    dlq_end = cfg.window_end + timedelta(minutes=5)
    with psycopg2.connect(cfg.warehouse_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT event_name, subject, payload
                  FROM analytics.raw_event_landing
                 WHERE left(event_id, %s) = %s
                   AND subject_user_hash = %s
                   AND occurred_at >= %s
                   AND occurred_at <= %s
                   AND payload->'client'->>'platform' = 'ios'
                 ORDER BY occurred_at ASC
                """,
                (
                    len(cfg.event_id_prefix),
                    cfg.event_id_prefix,
                    cfg.subject_user_hash,
                    cfg.window_start,
                    cfg.window_end,
                ),
            )
            landing_rows = [
                {
                    "event_name": row[0],
                    "subject": normalize_json_value(row[1]),
                    "payload": normalize_json_value(row[2]),
                }
                for row in cur.fetchall()
            ]
            cur.execute(
                """
                SELECT count(*)
                  FROM analytics.raw_event_dlq
                 WHERE event_id IS NOT NULL
                   AND left(event_id, %s) = %s
                   AND created_at >= %s
                   AND created_at <= %s
                """,
                (len(cfg.event_id_prefix), cfg.event_id_prefix, cfg.window_start, dlq_end),
            )
            dlq_count = int(cur.fetchone()[0])
    return landing_rows, dlq_count


def success_criteria(
    counts: Counter[str],
    dlq_count: int,
    forbidden_hits: int,
    out_of_scope_names: list[str],
    cfg: CanaryConfig,
) -> dict[str, bool]:
    impressions = counts["feed_item_impression"] + counts["feed_item_viewable_impression"]
    interactions = sum(counts[name] for name in SAFE_INTERACTION_EVENT_NAMES)
    return {
        "seeded_privacy_flags_true": cfg.share_analytics and cfg.personalization_enabled,
        "at_least_one_app_session_started": counts["app_session_started"] >= 1,
        "at_least_two_screen_viewed": counts["screen_viewed"] >= 2,
        "at_least_five_feed_impressions": impressions >= 5,
        "at_least_one_feed_item_open": counts["feed_item_open"] >= 1,
        "at_least_one_safe_feed_interaction": interactions >= 1,
        "dlq_zero": dlq_count == 0,
        "no_forbidden_fields": forbidden_hits == 0,
        "only_allowed_event_names": not out_of_scope_names,
        "downstream_checks_passed": (
            cfg.dbt_status == "passed"
            and cfg.soda_status == "passed"
            and cfg.dagster_status == "passed"
        ),
    }


def count_forbidden_keys(value: Any) -> int:
    if isinstance(value, dict):
        total = 0
        for key, nested in value.items():
            normalized = str(key).strip().lower().replace("-", "_")
            if normalized in FORBIDDEN_KEYS:
                total += 1
            total += count_forbidden_keys(nested)
        return total
    if isinstance(value, list):
        return sum(count_forbidden_keys(item) for item in value)
    return 0


def normalize_json_value(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def validate_safe_ref(key: str, value: str, errors: list[str]) -> None:
    if not value:
        errors.append(f"{key} is required")
        return
    if not SAFE_REF_PATTERN.fullmatch(value):
        errors.append(f"{key} must be a bounded non-PII reference")
    lowered = value.lower()
    if "@" in value or "bearer" in lowered or "token" in lowered:
        errors.append(f"{key} must not contain contact or token-shaped data")


def validate_warehouse_dsn(value: str, errors: list[str]) -> None:
    if not value:
        errors.append("EMSI_DP_CANARY_WAREHOUSE_DSN is required")
        return
    lowered = value.lower()
    for marker in LOCAL_DSN_MARKERS:
        if marker in lowered:
            errors.append("EMSI_DP_CANARY_WAREHOUSE_DSN must not point at local/dev")
            break
    if not any(sslmode in lowered for sslmode in ("sslmode=require", "sslmode=verify-ca", "sslmode=verify-full")):
        errors.append("EMSI_DP_CANARY_WAREHOUSE_DSN must require TLS via sslmode")


def require_check_result(name: str, status: str, artifact: str, errors: list[str]) -> None:
    if status not in CHECK_STATUSES:
        errors.append(f"{name} status must be one of {', '.join(sorted(CHECK_STATUSES))}")
    if not artifact:
        errors.append(f"{name} artifact is required for capture")


def parse_time_env(key: str, errors: list[str]) -> datetime | None:
    value = env(key)
    if not value:
        errors.append(f"{key} is required")
        return None
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        errors.append(f"{key} must be ISO-8601 with timezone")
        return None
    if parsed.tzinfo is None:
        errors.append(f"{key} must include timezone")
        return None
    return parsed.astimezone(timezone.utc).replace(microsecond=0)


def bool_env(key: str, errors: list[str]) -> bool | None:
    value = env(key).lower()
    if value in {"true", "1", "yes"}:
        return True
    if value in {"false", "0", "no"}:
        return False
    errors.append(f"{key} must be true or false")
    return None


def env(key: str) -> str:
    return os.getenv(key, "").strip()


def isoformat_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


if __name__ == "__main__":
    raise SystemExit(main())
