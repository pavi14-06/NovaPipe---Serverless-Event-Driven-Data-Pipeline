"""
NovaPipe — Unit Tests: Transformer Lambda
"""

import json
import sys
import os
import time
import unittest
from datetime import datetime, timezone, timedelta

sys.modules['boto3'] = __import__('unittest').mock.MagicMock()

os.environ.setdefault("PIPELINE_ENV",        "test")
os.environ.setdefault("PII_MASKING",         "true")
os.environ.setdefault("MAX_PAYLOAD_BYTES",   "524288")

import lambda_.transformer.handler as transformer


def _make_valid_envelope(**overrides):
    """Return a valid NovaPipe event envelope."""
    base = {
        "event_id":       "evt-test-12345",
        "source":         "api_gateway",
        "pipeline":       "test",
        "timestamp":      datetime.now(timezone.utc).isoformat(),
        "epoch_ms":       int(time.time() * 1000),
        "payload":        {"action": "purchase", "amount": 99.99},
        "schema_version": "1.0",
    }
    base.update(overrides)
    return base


class FakeContext:
    aws_request_id = "test-ctx-001"


class TestValidation(unittest.TestCase):

    def test_valid_envelope_passes(self):
        transformer._validate(_make_valid_envelope())  # should not raise

    def test_missing_event_id_raises(self):
        env = _make_valid_envelope()
        del env["event_id"]
        with self.assertRaises(transformer.ValidationError):
            transformer._validate(env)

    def test_missing_payload_raises(self):
        env = _make_valid_envelope()
        del env["payload"]
        with self.assertRaises(transformer.ValidationError):
            transformer._validate(env)

    def test_payload_not_dict_raises(self):
        env = _make_valid_envelope(payload="not-a-dict")
        with self.assertRaises(transformer.ValidationError):
            transformer._validate(env)

    def test_invalid_timestamp_raises(self):
        env = _make_valid_envelope(timestamp="not-a-timestamp")
        with self.assertRaises(transformer.ValidationError):
            transformer._validate(env)

    def test_future_epoch_ms_raises(self):
        future_ms = int(time.time() * 1000) + 999_999
        env = _make_valid_envelope(epoch_ms=future_ms)
        with self.assertRaises(transformer.ValidationError):
            transformer._validate(env)

    def test_stale_epoch_ms_raises(self):
        stale_ms = int((time.time() - 8 * 86400) * 1000)  # 8 days ago
        env = _make_valid_envelope(epoch_ms=stale_ms)
        with self.assertRaises(transformer.ValidationError):
            transformer._validate(env)


class TestPIIMasking(unittest.TestCase):

    def test_email_is_masked(self):
        result = transformer._mask_pii_value("Contact john.doe@example.com for support")
        self.assertNotIn("john.doe@example.com", result)
        self.assertIn("[EMAIL:", result)

    def test_phone_is_masked(self):
        result = transformer._mask_pii_value("Call us at 555-867-5309")
        self.assertNotIn("555-867-5309", result)
        self.assertIn("[PHONE:", result)

    def test_ssn_is_masked(self):
        result = transformer._mask_pii_value("SSN: 123-45-6789")
        self.assertNotIn("123-45-6789", result)
        self.assertIn("[SSN:", result)

    def test_non_pii_preserved(self):
        text = "Product: Widget X, Price: $19.99"
        result = transformer._mask_pii_value(text)
        self.assertIn("Widget X", result)
        self.assertIn("$19.99", result)

    def test_recursive_masking_in_nested_dict(self):
        payload = {
            "user":  {"email": "user@test.com", "name": "Alice"},
            "items": [{"sku": "SKU-001"}],
        }
        masked = transformer._mask_pii_recursive(payload)
        self.assertNotIn("user@test.com", json.dumps(masked))
        self.assertIn("Alice", json.dumps(masked))   # names not PII-masked


class TestEnrichment(unittest.TestCase):

    def test_enriched_has_fingerprint(self):
        env = _make_valid_envelope()
        result = transformer._enrich(env)
        self.assertIn("payload_fingerprint", result)
        self.assertEqual(len(result["payload_fingerprint"]), 64)  # SHA-256 hex

    def test_enriched_has_processed_at(self):
        result = transformer._enrich(_make_valid_envelope())
        self.assertIn("processed_at", result)
        # Should be parseable
        datetime.fromisoformat(result["processed_at"])

    def test_enriched_payload_keys_extracted(self):
        env = _make_valid_envelope(payload={"action": "buy", "amount": 50})
        result = transformer._enrich(env)
        self.assertIn("action", result["payload_keys"])
        self.assertIn("amount", result["payload_keys"])

    def test_fingerprint_deterministic(self):
        env = _make_valid_envelope()
        r1  = transformer._enrich(env)
        r2  = transformer._enrich(env)
        self.assertEqual(r1["payload_fingerprint"], r2["payload_fingerprint"])


class TestFilter(unittest.TestCase):

    def test_test_event_dropped_in_production(self):
        import lambda_.transformer.handler as h
        original = h.PIPELINE_ENV
        h.PIPELINE_ENV = "production"
        try:
            env    = _make_valid_envelope(payload={"_test": True})
            drop, reason = transformer._should_drop(env)
            self.assertTrue(drop)
            self.assertIn("test", reason.lower())
        finally:
            h.PIPELINE_ENV = original

    def test_normal_event_not_dropped(self):
        env         = _make_valid_envelope()
        drop, reason = transformer._should_drop(env)
        self.assertFalse(drop)


class TestHandlerIntegration(unittest.TestCase):

    def test_single_valid_event_transformed(self):
        env  = _make_valid_envelope()
        resp = transformer.handler(env, FakeContext())
        self.assertEqual(resp["statusCode"], 200)
        body = json.loads(resp["body"])
        self.assertEqual(len(body["transformed"]), 1)
        self.assertEqual(len(body["invalid"]),     0)

    def test_batch_of_events(self):
        events = [_make_valid_envelope() for _ in range(5)]
        resp   = transformer.handler(events, FakeContext())
        body   = json.loads(resp["body"])
        self.assertEqual(len(body["transformed"]), 5)

    def test_invalid_event_segregated(self):
        bad_env = {"event_id": "bad", "source": "test"}  # missing required fields
        resp    = transformer.handler(bad_env, FakeContext())
        body    = json.loads(resp["body"])
        self.assertEqual(len(body["invalid"]), 1)
        self.assertEqual(len(body["transformed"]), 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
