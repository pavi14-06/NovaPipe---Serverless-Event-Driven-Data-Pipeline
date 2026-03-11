"""
NovaPipe — Kafka Producer Lambda
Receives events from API Gateway / EventBridge and pushes to Kafka topic.
"""

import json
import os
import uuid
import logging
import time
from datetime import datetime, timezone
from typing import Any

import boto3
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ─── Logging ──────────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ─── Environment ──────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"].split(",")
KAFKA_TOPIC             = os.environ.get("KAFKA_TOPIC", "novapipe.events")
PIPELINE_ENV            = os.environ.get("PIPELINE_ENV", "production")
SECRET_NAME             = os.environ.get("KAFKA_SECRET_NAME", "novapipe/kafka")

# ─── Globals (reused across warm invocations) ─────────────────────────────────
_producer: KafkaProducer | None = None
_secrets_client = boto3.client("secretsmanager")


def _get_kafka_credentials() -> dict:
    """Fetch Kafka credentials from AWS Secrets Manager."""
    try:
        response = _secrets_client.get_secret_value(SecretId=SECRET_NAME)
        return json.loads(response["SecretString"])
    except Exception as exc:
        logger.warning("Could not fetch Kafka secret, using no-auth: %s", exc)
        return {}


def _get_producer() -> KafkaProducer:
    """Return a reusable KafkaProducer (singleton per Lambda container)."""
    global _producer
    if _producer is None:
        creds = _get_kafka_credentials()
        kwargs: dict[str, Any] = {
            "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
            "value_serializer": lambda v: json.dumps(v).encode("utf-8"),
            "key_serializer": lambda k: k.encode("utf-8") if k else None,
            "acks": "all",                  # strongest durability guarantee
            "retries": 5,
            "retry_backoff_ms": 300,
            "compression_type": "gzip",
            "linger_ms": 10,                # micro-batch for throughput
            "batch_size": 65536,            # 64 KB batches
        }
        if creds.get("username"):
            kwargs.update({
                "security_protocol": "SASL_SSL",
                "sasl_mechanism": "PLAIN",
                "sasl_plain_username": creds["username"],
                "sasl_plain_password": creds["password"],
            })
        _producer = KafkaProducer(**kwargs)
        logger.info("KafkaProducer initialized → %s", KAFKA_BOOTSTRAP_SERVERS)
    return _producer


def _build_envelope(raw_event: dict, source: str) -> dict:
    """Wrap a raw event in a standardized NovaPipe envelope."""
    return {
        "event_id":   str(uuid.uuid4()),
        "source":     source,
        "pipeline":   PIPELINE_ENV,
        "timestamp":  datetime.now(timezone.utc).isoformat(),
        "epoch_ms":   int(time.time() * 1000),
        "payload":    raw_event,
        "schema_version": "1.0",
    }


def _publish(envelope: dict) -> bool:
    """Publish a single envelope to Kafka. Returns True on success."""
    producer = _get_producer()
    event_id = envelope["event_id"]

    future = producer.send(
        topic=KAFKA_TOPIC,
        key=event_id,
        value=envelope,
    )
    try:
        record_metadata = future.get(timeout=10)
        logger.info(
            "Published event_id=%s → topic=%s partition=%d offset=%d",
            event_id,
            record_metadata.topic,
            record_metadata.partition,
            record_metadata.offset,
        )
        return True
    except KafkaError as exc:
        logger.error("Failed to publish event_id=%s: %s", event_id, exc)
        return False


def _parse_trigger_source(event: dict) -> tuple[list[dict], str]:
    """
    Detect the Lambda trigger type and extract raw records.
    Supports: API Gateway, EventBridge, SNS, direct invocation.
    """
    # API Gateway (HTTP)
    if "httpMethod" in event or "requestContext" in event:
        body = json.loads(event.get("body") or "{}")
        records = body if isinstance(body, list) else [body]
        return records, "api_gateway"

    # EventBridge / CloudWatch Events
    if "source" in event and "detail" in event:
        return [event["detail"]], f"eventbridge:{event['source']}"

    # SNS
    if "Records" in event and event["Records"][0].get("EventSource") == "aws:sns":
        records = [json.loads(r["Sns"]["Message"]) for r in event["Records"]]
        return records, "sns"

    # Direct invocation / test
    records = event if isinstance(event, list) else [event]
    return records, "direct"


# ─── Lambda Handler ────────────────────────────────────────────────────────────

def handler(event: dict, context: Any) -> dict:
    """
    Main Lambda entry point.

    Returns HTTP-compatible response with publish summary.
    """
    logger.info("Invocation started | request_id=%s", context.aws_request_id)

    raw_records, source = _parse_trigger_source(event)
    logger.info("Detected %d record(s) from source=%s", len(raw_records), source)

    results = {"published": 0, "failed": 0, "event_ids": []}

    for raw in raw_records:
        if not isinstance(raw, dict):
            logger.warning("Skipping non-dict record: %s", raw)
            results["failed"] += 1
            continue

        envelope = _build_envelope(raw, source)
        success  = _publish(envelope)

        if success:
            results["published"] += 1
            results["event_ids"].append(envelope["event_id"])
        else:
            results["failed"] += 1

    # Flush any buffered messages before Lambda freezes
    _get_producer().flush(timeout=15)

    status_code = 200 if results["failed"] == 0 else 207  # 207 = partial success

    logger.info(
        "Invocation complete | published=%d failed=%d",
        results["published"],
        results["failed"],
    )

    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(results),
    }
