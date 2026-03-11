"""
NovaPipe — Unit Tests: Producer Lambda
"""

import json
import time
import unittest
from unittest.mock import MagicMock, patch, call
from datetime import datetime, timezone

# Patch boto3 and kafka before importing handler
import sys
sys.modules['boto3']                 = MagicMock()
sys.modules['kafka']                 = MagicMock()
sys.modules['kafka.errors']          = MagicMock()

import importlib
import os
os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
os.environ.setdefault("KAFKA_TOPIC",             "novapipe.events")
os.environ.setdefault("PIPELINE_ENV",            "test")

# Import handler after env is set
import lambda_.producer.handler as producer_handler


class FakeLambdaContext:
    aws_request_id = "test-request-id-123"
    function_name  = "novapipe-producer-test"
    memory_limit_in_mb = 512


class TestBuildEnvelope(unittest.TestCase):

    def test_envelope_has_required_fields(self):
        raw = {"type": "click", "user_id": "u123"}
        env = producer_handler._build_envelope(raw, "api_gateway")

        self.assertIn("event_id",       env)
        self.assertIn("source",         env)
        self.assertIn("timestamp",      env)
        self.assertIn("epoch_ms",       env)
        self.assertIn("payload",        env)
        self.assertIn("schema_version", env)

    def test_envelope_source_set_correctly(self):
        env = producer_handler._build_envelope({}, "sns")
        self.assertEqual(env["source"], "sns")

    def test_envelope_payload_preserved(self):
        raw = {"key": "value", "nested": {"a": 1}}
        env = producer_handler._build_envelope(raw, "direct")
        self.assertEqual(env["payload"], raw)

    def test_epoch_ms_is_recent(self):
        env = producer_handler._build_envelope({}, "test")
        now = int(time.time() * 1000)
        self.assertAlmostEqual(env["epoch_ms"], now, delta=2000)

    def test_event_id_is_unique(self):
        ids = {producer_handler._build_envelope({}, "test")["event_id"] for _ in range(100)}
        self.assertEqual(len(ids), 100)


class TestParseTriggerSource(unittest.TestCase):

    def test_api_gateway_single_event(self):
        event = {
            "httpMethod": "POST",
            "body": json.dumps({"type": "purchase"}),
        }
        records, source = producer_handler._parse_trigger_source(event)
        self.assertEqual(source, "api_gateway")
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["type"], "purchase")

    def test_api_gateway_batch_in_body(self):
        events = [{"id": i} for i in range(5)]
        event  = {"httpMethod": "POST", "body": json.dumps(events)}
        records, source = producer_handler._parse_trigger_source(event)
        self.assertEqual(len(records), 5)

    def test_eventbridge_trigger(self):
        event = {
            "source":  "com.myapp.orders",
            "detail":  {"order_id": "ord-001"},
            "detail-type": "OrderCreated",
        }
        records, source = producer_handler._parse_trigger_source(event)
        self.assertIn("eventbridge", source)
        self.assertEqual(records[0]["order_id"], "ord-001")

    def test_direct_invocation_dict(self):
        event = {"custom_field": "custom_value"}
        records, source = producer_handler._parse_trigger_source(event)
        self.assertEqual(source, "direct")
        self.assertEqual(records[0]["custom_field"], "custom_value")

    def test_direct_invocation_list(self):
        event = [{"id": 1}, {"id": 2}]
        records, source = producer_handler._parse_trigger_source(event)
        self.assertEqual(len(records), 2)


class TestHandlerResponse(unittest.TestCase):

    def _make_mock_producer(self, success: bool = True):
        mock_future = MagicMock()
        if success:
            mock_meta = MagicMock()
            mock_meta.topic     = "novapipe.events"
            mock_meta.partition = 0
            mock_meta.offset    = 42
            mock_future.get.return_value = mock_meta
        else:
            from kafka.errors import KafkaError
            mock_future.get.side_effect = KafkaError("broker down")

        mock_prod = MagicMock()
        mock_prod.send.return_value = mock_future
        return mock_prod

    def test_handler_returns_200_on_success(self):
        with patch.object(producer_handler, "_get_producer", return_value=self._make_mock_producer(True)):
            resp = producer_handler.handler(
                {"type": "test_event"},
                FakeLambdaContext(),
            )
        self.assertEqual(resp["statusCode"], 200)
        body = json.loads(resp["body"])
        self.assertEqual(body["published"], 1)
        self.assertEqual(body["failed"], 0)

    def test_handler_returns_207_on_partial_failure(self):
        call_count = [0]
        def alternating_producer():
            call_count[0] += 1
            return self._make_mock_producer(call_count[0] % 2 == 0)

        event = [{"id": i} for i in range(4)]
        with patch.object(producer_handler, "_get_producer", side_effect=alternating_producer):
            resp = producer_handler.handler(event, FakeLambdaContext())

        self.assertIn(resp["statusCode"], [200, 207])

    def test_handler_skips_non_dict_records(self):
        with patch.object(producer_handler, "_get_producer", return_value=self._make_mock_producer(True)):
            resp = producer_handler.handler(
                ["not-a-dict", 123, None],
                FakeLambdaContext(),
            )
        body = json.loads(resp["body"])
        self.assertEqual(body["published"], 0)
        self.assertEqual(body["failed"], 3)


if __name__ == "__main__":
    unittest.main(verbosity=2)
