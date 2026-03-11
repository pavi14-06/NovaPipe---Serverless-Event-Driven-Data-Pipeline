"""
NovaPipe — Unit Tests: Consumer Lambda
"""

import base64
import json
import sys
import os
import time
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

# Mock heavy dependencies
sys.modules['boto3']              = MagicMock()
sys.modules['cassandra']          = MagicMock()
sys.modules['cassandra.cluster']  = MagicMock()
sys.modules['cassandra.policies'] = MagicMock()
sys.modules['cassandra.auth']     = MagicMock()
sys.modules['cassandra.query']    = MagicMock()

os.environ.setdefault("CASSANDRA_CONTACT_POINTS", "localhost")
os.environ.setdefault("CASSANDRA_KEYSPACE",       "novapipe")
os.environ.setdefault("S3_BUCKET_NAME",           "test-bucket")
os.environ.setdefault("DLQ_URL",                  "https://sqs.test/dlq")
os.environ.setdefault("PIPELINE_ENV",             "test")

import lambda_.consumer.handler as consumer


def _make_envelope(**overrides):
    base = {
        "event_id":       "evt-consumer-001",
        "source":         "api_gateway",
        "pipeline":       "test",
        "timestamp":      datetime.now(timezone.utc).isoformat(),
        "epoch_ms":       int(time.time() * 1000),
        "payload":        {"type": "order", "order_id": "ord-001"},
        "schema_version": "1.0",
    }
    base.update(overrides)
    return base


def _kafka_record(envelope: dict) -> dict:
    """Simulate a Kafka event source mapping record."""
    encoded = base64.b64encode(json.dumps(envelope).encode()).decode()
    return {
        "topic":     "novapipe.events",
        "partition": 0,
        "offset":    100,
        "key":       envelope["event_id"],
        "value":     encoded,
    }


class FakeContext:
    aws_request_id = "consumer-ctx-001"


class TestProcessRecord(unittest.TestCase):

    def test_valid_record_returns_ok(self):
        env    = _make_envelope()
        record = _kafka_record(env)

        with patch.object(consumer, "_write_to_cassandra", return_value=True), \
             patch.object(consumer, "_archive_to_s3", return_value=True):
            ok, reason = consumer._process_record(record)

        self.assertTrue(ok)
        self.assertEqual(reason, "ok")

    def test_cassandra_failure_returns_false(self):
        record = _kafka_record(_make_envelope())

        with patch.object(consumer, "_write_to_cassandra", return_value=False), \
             patch.object(consumer, "_archive_to_s3", return_value=True):
            ok, reason = consumer._process_record(record)

        self.assertFalse(ok)
        self.assertIn("cassandra", reason.lower())

    def test_s3_failure_is_non_fatal(self):
        record = _kafka_record(_make_envelope())

        with patch.object(consumer, "_write_to_cassandra", return_value=True), \
             patch.object(consumer, "_archive_to_s3", return_value=False):
            ok, reason = consumer._process_record(record)

        # Cassandra succeeded → overall success (S3 is best-effort)
        self.assertTrue(ok)
        self.assertEqual(reason, "cassandra_only")

    def test_invalid_json_returns_false(self):
        record = {"value": base64.b64encode(b"not-valid-json").decode(), "offset": 99}
        ok, reason = consumer._process_record(record)
        self.assertFalse(ok)
        self.assertIn("JSON", reason)

    def test_missing_required_fields_returns_false(self):
        bad_env = {"only_this": "field"}
        record  = _kafka_record(bad_env)
        ok, reason = consumer._process_record(record)
        self.assertFalse(ok)
        self.assertIn("Missing", reason)


class TestHandler(unittest.TestCase):

    def _make_kafka_event(self, count: int = 3) -> dict:
        records = [_kafka_record(_make_envelope()) for _ in range(count)]
        return {"records": {"novapipe.events-0": records}}

    def test_handler_success(self):
        event = self._make_kafka_event(3)

        with patch.object(consumer, "_process_record", return_value=(True, "ok")), \
             patch.object(consumer, "_send_to_dlq"):
            resp = consumer.handler(event, FakeContext())

        self.assertEqual(resp["statusCode"], 200)
        body = json.loads(resp["body"])
        self.assertEqual(body["success"], 3)
        self.assertEqual(body["failed"],  0)

    def test_handler_routes_failures_to_dlq(self):
        event = self._make_kafka_event(4)

        def _alternating(record):
            # Fail every other record
            offset = record.get("offset", 0) % 2
            return (offset == 0, "ok" if offset == 0 else "forced_failure")

        with patch.object(consumer, "_process_record", side_effect=_alternating), \
             patch.object(consumer, "_send_to_dlq") as mock_dlq:
            resp = consumer.handler(event, FakeContext())

        body = json.loads(resp["body"])
        self.assertGreater(body["dlq_sent"], 0)

    def test_handler_raises_when_all_fail(self):
        event = self._make_kafka_event(2)

        with patch.object(consumer, "_process_record", return_value=(False, "all_fail")), \
             patch.object(consumer, "_send_to_dlq"):
            with self.assertRaises(RuntimeError):
                consumer.handler(event, FakeContext())

    def test_handler_handles_empty_event(self):
        resp = consumer.handler({"records": {}}, FakeContext())
        body = json.loads(resp["body"])
        self.assertEqual(body["total"],   0)
        self.assertEqual(body["success"], 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
