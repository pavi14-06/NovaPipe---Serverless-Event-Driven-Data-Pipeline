"""
NovaPipe — Transformer Lambda
Enriches, filters, validates, and normalizes event envelopes
before they reach the consumer. Triggered between Producer → Consumer.
"""

import json
import os
import re
import logging
import hashlib
import ipaddress
from datetime import datetime, timezone
from typing import Any

import boto3

# ─── Logging ──────────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ─── Environment ──────────────────────────────────────────────────────────────
PIPELINE_ENV        = os.environ.get("PIPELINE_ENV", "production")
GEO_ENRICHMENT      = os.environ.get("GEO_ENRICHMENT", "false").lower() == "true"
PII_MASKING         = os.environ.get("PII_MASKING", "true").lower() == "true"
MAX_PAYLOAD_BYTES   = int(os.environ.get("MAX_PAYLOAD_BYTES", str(1024 * 512)))  # 512 KB
FILTER_SOURCES      = set(os.environ.get("FILTER_SOURCES", "").split(",")) - {""}

_ssm = boto3.client("ssm")

# ─── PII Patterns ─────────────────────────────────────────────────────────────
_PII_PATTERNS = {
    "email":       re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}"),
    "phone":       re.compile(r"\b(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b"),
    "ssn":         re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
    "credit_card": re.compile(r"\b(?:\d[ -]?){13,16}\b"),
    "ip_v4":       re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b"),
}


def _mask_pii_value(value: str) -> str:
    """Replace PII patterns in a string with masked placeholders."""
    for pii_type, pattern in _PII_PATTERNS.items():
        def _replace(match, pt=pii_type):
            original = match.group()
            hashed   = hashlib.sha256(original.encode()).hexdigest()[:8]
            return f"[{pt.upper()}:{hashed}]"
        value = pattern.sub(_replace, value)
    return value


def _mask_pii_recursive(obj: Any) -> Any:
    """Recursively mask PII in nested dicts/lists."""
    if isinstance(obj, dict):
        return {k: _mask_pii_recursive(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_mask_pii_recursive(i) for i in obj]
    elif isinstance(obj, str):
        return _mask_pii_value(obj)
    return obj


# ─── Enrichment ───────────────────────────────────────────────────────────────

def _enrich(envelope: dict) -> dict:
    """Add derived fields to the envelope."""
    enriched = envelope.copy()

    # Compute payload fingerprint for deduplication
    payload_str = json.dumps(envelope.get("payload", {}), sort_keys=True)
    enriched["payload_fingerprint"] = hashlib.sha256(payload_str.encode()).hexdigest()

    # Add processing metadata
    enriched["processed_at"]  = datetime.now(timezone.utc).isoformat()
    enriched["processed_by"]  = "novapipe-transformer"
    enriched["pipeline_env"]  = PIPELINE_ENV

    # Payload size tracking
    enriched["payload_bytes"] = len(payload_str.encode("utf-8"))

    # Extract top-level payload keys for indexing
    enriched["payload_keys"] = list(envelope.get("payload", {}).keys())

    return enriched


# ─── Validation ───────────────────────────────────────────────────────────────

class ValidationError(Exception):
    pass


def _validate(envelope: dict) -> None:
    """Raise ValidationError if the envelope fails schema checks."""
    required = {"event_id", "source", "timestamp", "payload", "epoch_ms"}
    missing  = required - envelope.keys()
    if missing:
        raise ValidationError(f"Missing required fields: {missing}")

    # Timestamp must be parseable ISO 8601
    try:
        datetime.fromisoformat(envelope["timestamp"])
    except (ValueError, TypeError):
        raise ValidationError(f"Invalid timestamp: {envelope['timestamp']!r}")

    # epoch_ms sanity: not in the future by more than 60s, not older than 7 days
    now_ms      = int(datetime.now(timezone.utc).timestamp() * 1000)
    event_epoch = int(envelope["epoch_ms"])
    if event_epoch > now_ms + 60_000:
        raise ValidationError(f"epoch_ms is in the future: {event_epoch}")
    if now_ms - event_epoch > 7 * 24 * 3600 * 1000:
        raise ValidationError(f"epoch_ms is older than 7 days: {event_epoch}")

    # Payload must be a dict
    if not isinstance(envelope.get("payload"), dict):
        raise ValidationError("payload must be a JSON object")

    # Payload size guard
    payload_bytes = len(json.dumps(envelope["payload"]).encode())
    if payload_bytes > MAX_PAYLOAD_BYTES:
        raise ValidationError(
            f"Payload too large: {payload_bytes} bytes > {MAX_PAYLOAD_BYTES} limit"
        )


# ─── Filter ───────────────────────────────────────────────────────────────────

def _should_drop(envelope: dict) -> tuple[bool, str]:
    """Return (True, reason) if the event should be dropped silently."""
    # Source-based filtering
    if FILTER_SOURCES and envelope.get("source") in FILTER_SOURCES:
        return True, f"source '{envelope['source']}' is in filter list"

    # Drop test events in production
    payload = envelope.get("payload", {})
    if PIPELINE_ENV == "production" and payload.get("_test") is True:
        return True, "test event dropped in production"

    return False, ""


# ─── Lambda Handler ────────────────────────────────────────────────────────────

def handler(event: dict, context: Any) -> dict:
    """
    Triggered synchronously (or via Kafka trigger) before consumer.
    Accepts a single envelope or a list of envelopes.
    Returns transformed envelopes.
    """
    logger.info("Transformer invoked | request_id=%s", context.aws_request_id)

    raw_envelopes = event if isinstance(event, list) else [event]

    results = {
        "transformed": [],
        "dropped":     [],
        "invalid":     [],
    }

    for envelope in raw_envelopes:
        event_id = envelope.get("event_id", "UNKNOWN")

        # 1. Validate
        try:
            _validate(envelope)
        except ValidationError as exc:
            logger.warning("Invalid event_id=%s: %s", event_id, exc)
            results["invalid"].append({"event_id": event_id, "reason": str(exc)})
            continue

        # 2. Filter
        drop, reason = _should_drop(envelope)
        if drop:
            logger.info("Dropped event_id=%s: %s", event_id, reason)
            results["dropped"].append({"event_id": event_id, "reason": reason})
            continue

        # 3. PII Masking
        if PII_MASKING:
            envelope["payload"] = _mask_pii_recursive(envelope["payload"])

        # 4. Enrich
        transformed = _enrich(envelope)

        results["transformed"].append(transformed)
        logger.info("Transformed event_id=%s ✅", event_id)

    logger.info(
        "Transformer complete | transformed=%d dropped=%d invalid=%d",
        len(results["transformed"]),
        len(results["dropped"]),
        len(results["invalid"]),
    )

    return {
        "statusCode": 200,
        "body": json.dumps(results),
    }
