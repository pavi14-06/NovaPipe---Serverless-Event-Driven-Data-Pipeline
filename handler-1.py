"""
NovaPipe — Kafka Consumer Lambda
Consumes events from Kafka topic, writes hot data to Cassandra,
archives cold data to S3, and routes failures to SQS DLQ.
"""

import json
import os
import logging
import base64
import time
from datetime import datetime, timezone
from typing import Any

import boto3
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import DCAwareRoundRobinPolicy, RetryPolicy
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement, ConsistencyLevel

# ─── Logging ──────────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ─── Environment ──────────────────────────────────────────────────────────────
CASSANDRA_HOSTS      = os.environ["CASSANDRA_CONTACT_POINTS"].split(",")
CASSANDRA_PORT       = int(os.environ.get("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE   = os.environ.get("CASSANDRA_KEYSPACE", "novapipe")
CASSANDRA_SECRET     = os.environ.get("CASSANDRA_SECRET_NAME", "novapipe/cassandra")
S3_BUCKET            = os.environ["S3_BUCKET_NAME"]
DLQ_URL              = os.environ["DLQ_URL"]
COLD_THRESHOLD_HOURS = int(os.environ.get("COLD_THRESHOLD_HOURS", "24"))

# ─── AWS Clients ──────────────────────────────────────────────────────────────
_s3_client       = boto3.client("s3")
_sqs_client      = boto3.client("sqs")
_secrets_client  = boto3.client("secretsmanager")

# ─── Cassandra Session (singleton) ────────────────────────────────────────────
_cassandra_session = None


def _get_cassandra_credentials() -> dict:
    try:
        resp = _secrets_client.get_secret_value(SecretId=CASSANDRA_SECRET)
        return json.loads(resp["SecretString"])
    except Exception as exc:
        logger.warning("Could not fetch Cassandra secret: %s", exc)
        return {}


def _get_cassandra_session():
    global _cassandra_session
    if _cassandra_session is None:
        creds = _get_cassandra_credentials()
        auth  = None
        if creds.get("username"):
            auth = PlainTextAuthProvider(
                username=creds["username"],
                password=creds["password"],
            )

        profile = ExecutionProfile(
            load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="us-east-1"),
            retry_policy=RetryPolicy(),
            consistency_level=ConsistencyLevel.LOCAL_QUORUM,
            request_timeout=10.0,
        )

        cluster = Cluster(
            contact_points=CASSANDRA_HOSTS,
            port=CASSANDRA_PORT,
            auth_provider=auth,
            execution_profiles={EXEC_PROFILE_DEFAULT: profile},
            protocol_version=4,
        )
        _cassandra_session = cluster.connect(CASSANDRA_KEYSPACE)
        logger.info("Cassandra session established → keyspace=%s", CASSANDRA_KEYSPACE)

    return _cassandra_session


# ─── Cassandra Write ───────────────────────────────────────────────────────────

INSERT_CQL = """
    INSERT INTO events (
        event_id, source, pipeline, event_timestamp,
        epoch_ms, payload, schema_version, ingested_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""

_prepared_stmt = None


def _get_prepared_stmt():
    global _prepared_stmt
    if _prepared_stmt is None:
        session       = _get_cassandra_session()
        _prepared_stmt = session.prepare(INSERT_CQL)
    return _prepared_stmt


def _write_to_cassandra(envelope: dict) -> bool:
    try:
        session = _get_cassandra_session()
        stmt    = _get_prepared_stmt()
        session.execute(stmt, (
            envelope["event_id"],
            envelope["source"],
            envelope["pipeline"],
            datetime.fromisoformat(envelope["timestamp"]),
            envelope["epoch_ms"],
            json.dumps(envelope["payload"]),
            envelope.get("schema_version", "1.0"),
            datetime.now(timezone.utc),
        ))
        return True
    except Exception as exc:
        logger.error("Cassandra write failed for event_id=%s: %s", envelope.get("event_id"), exc)
        return False


# ─── S3 Archive ───────────────────────────────────────────────────────────────

def _archive_to_s3(envelope: dict) -> bool:
    """Write event to S3 partitioned by date for Athena / Glue compatibility."""
    try:
        ts   = datetime.fromisoformat(envelope["timestamp"])
        key  = (
            f"events/year={ts.year}/month={ts.month:02d}/"
            f"day={ts.day:02d}/hour={ts.hour:02d}/"
            f"{envelope['event_id']}.json"
        )
        _s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=json.dumps(envelope).encode("utf-8"),
            ContentType="application/json",
            Metadata={
                "pipeline":  envelope.get("pipeline", ""),
                "source":    envelope.get("source", ""),
                "event_id":  envelope["event_id"],
            },
        )
        logger.info("Archived event_id=%s → s3://%s/%s", envelope["event_id"], S3_BUCKET, key)
        return True
    except Exception as exc:
        logger.error("S3 archive failed for event_id=%s: %s", envelope.get("event_id"), exc)
        return False


# ─── DLQ Routing ──────────────────────────────────────────────────────────────

def _send_to_dlq(raw_record: dict, reason: str) -> None:
    try:
        _sqs_client.send_message(
            QueueUrl=DLQ_URL,
            MessageBody=json.dumps({
                "failed_record": raw_record,
                "reason":        reason,
                "failed_at":     datetime.now(timezone.utc).isoformat(),
            }),
        )
        logger.warning("Sent failed record to DLQ | reason=%s", reason)
    except Exception as exc:
        logger.error("Could not send to DLQ: %s", exc)


# ─── Record Processing ────────────────────────────────────────────────────────

def _process_record(kafka_record: dict) -> tuple[bool, str]:
    """
    Decode a single Kafka record, write to Cassandra + S3.
    Returns (success, reason).
    """
    try:
        raw_value = kafka_record.get("value", "")
        # Kafka triggers send base64-encoded values
        if isinstance(raw_value, str):
            decoded = base64.b64decode(raw_value).decode("utf-8")
            envelope = json.loads(decoded)
        else:
            envelope = raw_value

        # Validate envelope structure
        required = {"event_id", "source", "timestamp", "payload"}
        missing  = required - envelope.keys()
        if missing:
            return False, f"Missing fields: {missing}"

        cassandra_ok = _write_to_cassandra(envelope)
        s3_ok        = _archive_to_s3(envelope)

        if cassandra_ok and s3_ok:
            return True, "ok"
        elif cassandra_ok:
            return True, "cassandra_only"   # S3 failure is non-fatal
        else:
            return False, "cassandra_write_failed"

    except json.JSONDecodeError as exc:
        return False, f"JSON decode error: {exc}"
    except Exception as exc:
        return False, f"Unexpected error: {exc}"


# ─── Lambda Handler ────────────────────────────────────────────────────────────

def handler(event: dict, context: Any) -> dict:
    """
    Triggered by Kafka (AWS MSK) event source mapping.
    Each invocation receives a batch of records from one or more partitions.
    """
    logger.info("Consumer invoked | request_id=%s", context.aws_request_id)

    summary = {
        "total":    0,
        "success":  0,
        "failed":   0,
        "dlq_sent": 0,
    }

    # event["records"] → { "topic-partition": [ {key, value, offset, ...} ] }
    for partition_key, records in event.get("records", {}).items():
        logger.info("Processing partition=%s | records=%d", partition_key, len(records))

        for record in records:
            summary["total"] += 1
            start = time.monotonic()

            ok, reason = _process_record(record)

            elapsed_ms = int((time.monotonic() - start) * 1000)
            offset     = record.get("offset", "?")

            if ok:
                summary["success"] += 1
                logger.info(
                    "✅ offset=%s processed in %dms | reason=%s",
                    offset, elapsed_ms, reason,
                )
            else:
                summary["failed"] += 1
                logger.error("❌ offset=%s failed | reason=%s", offset, reason)
                _send_to_dlq(record, reason)
                summary["dlq_sent"] += 1

    logger.info(
        "Consumer complete | total=%d success=%d failed=%d dlq=%d",
        summary["total"], summary["success"], summary["failed"], summary["dlq_sent"],
    )

    # Returning non-200 causes Lambda to retry the batch
    if summary["failed"] > 0 and summary["success"] == 0:
        raise RuntimeError(f"All {summary['failed']} records failed — triggering retry")

    return {"statusCode": 200, "body": json.dumps(summary)}
