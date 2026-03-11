"""
NovaPipe — S3 Archiver
Handles archiving event data to S3 data lake with
Hive-compatible partitioning for Athena/Glue queries.
Supports batch uploads, compression, and manifest generation.
"""

from __future__ import annotations

import gzip
import io
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterator
from uuid import uuid4

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

# ─── Config ────────────────────────────────────────────────────────────────────

S3_BUCKET       = os.environ.get("S3_BUCKET_NAME", "novapipe-data-lake")
S3_PREFIX       = os.environ.get("S3_PREFIX", "events")
COMPRESS        = os.environ.get("S3_COMPRESS", "true").lower() == "true"
BATCH_SIZE      = int(os.environ.get("S3_BATCH_SIZE", "500"))
AWS_REGION      = os.environ.get("AWS_REGION", "us-east-1")

_s3 = boto3.client("s3", region_name=AWS_REGION)


# ─── Partition Key Builder ────────────────────────────────────────────────────

def _partition_prefix(ts: datetime, source: str) -> str:
    """
    Generate Hive-style partition path.
    e.g. events/source=api_gateway/year=2025/month=01/day=15/hour=12/
    """
    safe_source = source.replace("/", "_").replace(":", "_")
    return (
        f"{S3_PREFIX}/"
        f"source={safe_source}/"
        f"year={ts.year}/"
        f"month={ts.month:02d}/"
        f"day={ts.day:02d}/"
        f"hour={ts.hour:02d}/"
    )


def _s3_key(ts: datetime, source: str, batch_id: str, compressed: bool) -> str:
    ext = ".json.gz" if compressed else ".json"
    return f"{_partition_prefix(ts, source)}{batch_id}{ext}"


# ─── Upload Helpers ───────────────────────────────────────────────────────────

def _serialize_batch(events: list[dict], compress: bool = COMPRESS) -> bytes:
    """Serialize a list of events as newline-delimited JSON (optionally gzipped)."""
    ndjson = "\n".join(json.dumps(e, default=str) for e in events)
    raw    = ndjson.encode("utf-8")
    if compress:
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
            gz.write(raw)
        return buf.getvalue()
    return raw


def upload_event(envelope: dict) -> str:
    """
    Archive a single event envelope to S3.
    Returns the S3 key on success.
    """
    ts     = datetime.fromisoformat(envelope["timestamp"])
    source = envelope.get("source", "unknown")
    key    = _s3_key(ts, source, envelope["event_id"], COMPRESS)

    body          = json.dumps(envelope, default=str).encode("utf-8")
    content_type  = "application/json"

    if COMPRESS:
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
            gz.write(body)
        body         = buf.getvalue()
        content_type = "application/gzip"

    _s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=body,
        ContentType=content_type,
        Metadata={
            "event_id":  envelope["event_id"],
            "source":    source,
            "pipeline":  envelope.get("pipeline", ""),
        },
        ServerSideEncryption="AES256",
    )

    logger.debug("Archived → s3://%s/%s", S3_BUCKET, key)
    return key


def upload_batch(events: list[dict]) -> dict:
    """
    Upload multiple events as a single NDJSON file.
    Groups events by (source, hour) for efficient partitioning.

    Returns summary dict with keys_written and bytes_uploaded.
    """
    if not events:
        return {"keys_written": 0, "bytes_uploaded": 0}

    # Group by source + hour bucket
    groups: dict[str, list[dict]] = {}
    for ev in events:
        ts     = datetime.fromisoformat(ev["timestamp"])
        source = ev.get("source", "unknown")
        bucket = f"{source}|{ts.strftime('%Y%m%d%H')}"
        groups.setdefault(bucket, []).append(ev)

    keys_written  = 0
    bytes_uploaded = 0

    for bucket_key, group in groups.items():
        source, _ = bucket_key.split("|", 1)
        ts_ref    = datetime.fromisoformat(group[0]["timestamp"])
        batch_id  = str(uuid4())
        s3_key    = _s3_key(ts_ref, source, batch_id, COMPRESS)
        body      = _serialize_batch(group, COMPRESS)

        _s3.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=body,
            ContentType="application/gzip" if COMPRESS else "application/x-ndjson",
            Metadata={
                "record_count": str(len(group)),
                "source":       source,
            },
            ServerSideEncryption="AES256",
        )

        logger.info(
            "Batch archived → s3://%s/%s | records=%d bytes=%d",
            S3_BUCKET, s3_key, len(group), len(body),
        )
        keys_written  += 1
        bytes_uploaded += len(body)

    return {"keys_written": keys_written, "bytes_uploaded": bytes_uploaded}


def generate_manifest(date_prefix: str) -> dict:
    """
    Generate a manifest of all objects under a date prefix.
    Useful for Athena MSCK REPAIR TABLE or Glue crawlers.
    """
    prefix   = f"{S3_PREFIX}/{date_prefix}/"
    paginator = _s3.get_paginator("list_objects_v2")
    pages     = paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix)

    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "bucket":       S3_BUCKET,
        "prefix":       prefix,
        "entries":      [],
    }

    for page in pages:
        for obj in page.get("Contents", []):
            manifest["entries"].append({
                "key":           obj["Key"],
                "size_bytes":    obj["Size"],
                "last_modified": obj["LastModified"].isoformat(),
                "etag":          obj["ETag"].strip('"'),
            })

    manifest["total_files"] = len(manifest["entries"])
    manifest["total_bytes"] = sum(e["size_bytes"] for e in manifest["entries"])

    # Write manifest to S3
    manifest_key = f"manifests/{date_prefix}/manifest_{int(time.time())}.json"
    _s3.put_object(
        Bucket=S3_BUCKET,
        Key=manifest_key,
        Body=json.dumps(manifest, indent=2).encode("utf-8"),
        ContentType="application/json",
    )

    logger.info(
        "Manifest written → s3://%s/%s | files=%d bytes=%d",
        S3_BUCKET, manifest_key,
        manifest["total_files"],
        manifest["total_bytes"],
    )

    return manifest
