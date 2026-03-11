-- ============================================================
-- NovaPipe — Cassandra Schema
-- Keyspace & Tables optimized for high-throughput time-series
-- event writes and partition-key based reads.
-- ============================================================

-- ─── Keyspace ──────────────────────────────────────────────
CREATE KEYSPACE IF NOT EXISTS novapipe
    WITH replication = {
        'class':              'NetworkTopologyStrategy',
        'us-east-1':          3
    }
    AND durable_writes = true;

USE novapipe;

-- ─── Main Events Table ─────────────────────────────────────
-- Partition key: (source, year_month) — keeps partitions bounded
-- Clustering:    event_timestamp DESC — latest events first
CREATE TABLE IF NOT EXISTS events (
    source          text,
    year_month      text,           -- 'YYYY-MM' for partition sizing
    event_timestamp timestamp,
    event_id        uuid,
    pipeline        text,
    epoch_ms        bigint,
    payload         text,           -- JSON blob
    schema_version  text,
    ingested_at     timestamp,
    payload_bytes   int,
    payload_keys    list<text>,
    payload_fingerprint text,
    processed_at    timestamp,
    processed_by    text,
    PRIMARY KEY ((source, year_month), event_timestamp, event_id)
) WITH CLUSTERING ORDER BY (event_timestamp DESC, event_id ASC)
    AND compaction = {
        'class': 'TimeWindowCompactionStrategy',
        'compaction_window_unit': 'HOURS',
        'compaction_window_size': 1
    }
    AND gc_grace_seconds = 86400
    AND default_time_to_live = 2592000   -- 30 days TTL (cold data goes to S3)
    AND comment = 'Hot event store — 30 day TTL, partitioned by source+month';

-- ─── Deduplication Table ──────────────────────────────────
-- Lightweight writes to prevent duplicate processing
CREATE TABLE IF NOT EXISTS event_dedup (
    event_id        uuid        PRIMARY KEY,
    seen_at         timestamp,
    source          text
) WITH default_time_to_live = 86400   -- 24 hour dedup window
    AND comment = 'Event deduplication — 24h TTL';

-- ─── Pipeline Metrics Table ───────────────────────────────
-- Aggregated per-minute pipeline metrics
CREATE TABLE IF NOT EXISTS pipeline_metrics (
    pipeline        text,
    metric_date     date,
    minute_bucket   timestamp,
    source          text,
    events_total    counter,
    events_failed   counter,
    bytes_total     counter,
    PRIMARY KEY ((pipeline, metric_date), minute_bucket, source)
) WITH CLUSTERING ORDER BY (minute_bucket DESC, source ASC)
    AND comment = 'Aggregated per-minute pipeline counters';

-- ─── Dead Letter Table ────────────────────────────────────
-- Tracks failed events that were sent to DLQ
CREATE TABLE IF NOT EXISTS dead_letters (
    pipeline        text,
    failed_date     date,
    failed_at       timestamp,
    event_id        text,
    reason          text,
    raw_record      text,
    retry_count     int,
    resolved        boolean,
    PRIMARY KEY ((pipeline, failed_date), failed_at, event_id)
) WITH CLUSTERING ORDER BY (failed_at DESC, event_id ASC)
    AND default_time_to_live = 604800   -- 7 day retention
    AND comment = 'Failed events pending investigation';

-- ─── Schema Version Table ─────────────────────────────────
CREATE TABLE IF NOT EXISTS schema_registry (
    schema_name     text,
    version         text,
    created_at      timestamp,
    definition      text,
    PRIMARY KEY (schema_name, version)
) WITH CLUSTERING ORDER BY (version DESC)
    AND comment = 'Tracks event schema versions';

-- ─── Seed Schema Version ─────────────────────────────────
INSERT INTO schema_registry (schema_name, version, created_at, definition)
VALUES ('novapipe.event', '1.0', toTimestamp(now()),
        '{"fields":["event_id","source","timestamp","payload","schema_version"]}');

-- ─── Secondary Indexes ────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_events_fingerprint
    ON events (payload_fingerprint);

CREATE INDEX IF NOT EXISTS idx_dead_letters_resolved
    ON dead_letters (resolved);
