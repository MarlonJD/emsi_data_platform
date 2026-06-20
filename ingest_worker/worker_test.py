from __future__ import annotations

import json
import sys
import types
import unittest

if "kafka" not in sys.modules:
    kafka = types.ModuleType("kafka")
    kafka.KafkaConsumer = object
    kafka.KafkaProducer = object
    kafka.TopicPartition = object
    sys.modules["kafka"] = kafka

if "psycopg2" not in sys.modules:
    psycopg2 = types.ModuleType("psycopg2")

    class OperationalError(Exception):
        pass

    psycopg2.OperationalError = OperationalError
    psycopg2.connect = lambda *_args, **_kwargs: None
    psycopg2_extras = types.ModuleType("psycopg2.extras")
    psycopg2_extras.Json = lambda value: value
    sys.modules["psycopg2"] = psycopg2
    sys.modules["psycopg2.extras"] = psycopg2_extras

from ingest_worker import worker


class WorkerValidationTest(unittest.TestCase):
    def test_expansion_metadata_payload_is_accepted(self) -> None:
        event = valid_event(
            "admin_reveal_audit_recorded",
            {
                "admin_surface": "admin_console",
                "module_key": "users",
                "screen_key": "admin.users",
                "reveal_field_class": "contact",
                "reveal_result": "success",
                "actor_role_scope_key": "support.global",
                "reason_length_bucket": "21_100",
                "reason_category": "support_follow_up",
                "confirmation_state": "confirmed",
                "authorization_outcome": "allowed",
                "audit_action_key": "reveal_user_contact",
                "audit_receipt_hash": "sha256:audit",
                "target_type": "user",
                "target_hash": "sha256:target",
            },
        )

        validated = worker.validate_event(json.dumps(event).encode("utf-8"))

        self.assertEqual(validated.event_name, "admin_reveal_audit_recorded")

    def test_expansion_metadata_payload_rejects_raw_value_in_allowed_field(self) -> None:
        event = valid_event(
            "admin_reveal_audit_recorded",
            {"reveal_field_class": "+1-555-123-4567"},
        )

        with self.assertRaises(worker.ValidationError) as err:
            worker.validate_event(json.dumps(event).encode("utf-8"))

        self.assertEqual(err.exception.code, "raw_expansion_metadata_value")

    def test_expansion_metadata_payload_rejects_free_form_note_value(self) -> None:
        event = valid_event(
            "admin_note_metadata_recorded",
            {"note_surface": "application review private note"},
        )

        with self.assertRaises(worker.ValidationError) as err:
            worker.validate_event(json.dumps(event).encode("utf-8"))

        self.assertEqual(err.exception.code, "raw_expansion_metadata_value")

    def test_expansion_metadata_payload_rejects_raw_hash_label(self) -> None:
        event = valid_event(
            "admin_reveal_audit_recorded",
            {"reveal_field_class": "contact", "audit_receipt_hash": "sha256:private receipt note"},
        )

        with self.assertRaises(worker.ValidationError) as err:
            worker.validate_event(json.dumps(event).encode("utf-8"))

        self.assertEqual(err.exception.code, "invalid_expansion_metadata_hash")

    def test_expansion_metadata_payload_rejects_unsupported_key(self) -> None:
        event = valid_event(
            "admin_note_metadata_recorded",
            {"note_surface": "application_review", "note_summary": "private"},
        )

        with self.assertRaises(worker.ValidationError) as err:
            worker.validate_event(json.dumps(event).encode("utf-8"))

        self.assertEqual(err.exception.code, "unsupported_expansion_metadata_field")


def valid_event(event_name: str, payload: dict[str, object]) -> dict[str, object]:
    return {
        "event_id": "evt-expansion-1",
        "event_name": event_name,
        "event_version": 1,
        "occurred_at": "2026-06-20T12:00:00Z",
        "received_at": "2026-06-20T12:00:01Z",
        "producer": "emsi-go-api",
        "privacy_class": "pseudonymous",
        "consent_scope": ["analytics"],
        "subject": {"user_hash": "sha256:user", "session_id": "session-1"},
        "payload": payload,
    }


if __name__ == "__main__":
    unittest.main()
