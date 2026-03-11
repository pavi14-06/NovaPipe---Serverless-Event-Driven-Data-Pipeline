# NovaPipe — Architecture Deep Dive

## Overview

NovaPipe is a **serverless event-driven data pipeline** built for high-throughput, fault-tolerant, and scalable data ingestion. It decouples event producers from consumers using Apache Kafka as the central nervous system, with AWS Lambda handling stateless compute, Cassandra for hot storage, and S3 as the cold data lake.

---

## Component Breakdown

### 1. Producer Lambda (`lambda/producer/`)

**Responsibility:** Receive events from external sources and publish to Kafka.

**Trigger sources supported:**
- HTTP POST via API Gateway (V2)
- EventBridge scheduled or rule-based events
- SNS topic subscriptions
- Direct Lambda invocations (testing)

**Key design decisions:**
- KafkaProducer is a **module-level singleton** — reused across warm invocations to avoid reconnection overhead
- All events wrapped in a standardized **envelope** with metadata before publishing
- `acks="all"` ensures strongest durability (all ISR replicas acknowledge)
- Gzip compression + 64KB batching for throughput efficiency
- Secrets Manager integration for SASL auth credentials

---

### 2. Transformer Lambda (`lambda/transformer/`)

**Responsibility:** Validate, enrich, filter, and mask events before they hit storage.

**Processing pipeline:**
```
Raw Envelope → Validate → Filter → PII Masking → Enrich → Output
```

**PII Masking:** Regex-based detection and SHA-256-keyed masking for:
- Email addresses
- Phone numbers  
- Social Security Numbers
- Credit card numbers
- IPv4 addresses

**Enrichment fields added:**
- `payload_fingerprint` — SHA-256 for deduplication
- `processed_at` / `processed_by`
- `payload_bytes` — size tracking
- `payload_keys` — field index for querying

---

### 3. Consumer Lambda (`lambda/consumer/`)

**Responsibility:** Write validated events to Cassandra (hot) and S3 (cold), route failures to DLQ.

**Trigger:** AWS MSK (Managed Streaming for Kafka) event source mapping — Lambda scales automatically based on partition lag.

**Dual-write strategy:**
- **Cassandra** — Synchronous, critical path. Failure = retry.
- **S3** — Best-effort. Failure is non-fatal (logged as `cassandra_only`).

**Failure handling:**
- Failed events sent to SQS Dead Letter Queue with failure reason + timestamp
- If ALL records in a batch fail → raises `RuntimeError` → Lambda retries entire batch

---

### 4. Kafka Topics

| Topic | Partitions | Retention | Purpose |
|---|---|---|---|
| `novapipe.events` | 12 | 7 days | Raw event ingestion |
| `novapipe.events.transformed` | 12 | 3 days | Validated + enriched |
| `novapipe.events.dlq` | 3 | 30 days | Failed events (compacted) |
| `novapipe.metrics` | 6 | 1 day | Pipeline telemetry |
| `novapipe.audit` | 3 | 90 days | Immutable audit trail |

---

### 5. Cassandra Schema Design

**Partition strategy:** `(source, year_month)` — bounds partition size to one month per source, preventing hot partitions.

**Clustering:** `event_timestamp DESC` — latest events retrieved first, matching access patterns.

**TTL:** 30 days on the events table. Events older than 30 days are expected to exist in S3 (cold tier).

**TWCS compaction:** `TimeWindowCompactionStrategy` with 1-hour windows — optimal for time-series write-heavy workloads.

---

### 6. S3 Data Lake Structure

```
s3://novapipe-data-lake-{env}/
└── events/
    └── source={source}/
        └── year={YYYY}/
            └── month={MM}/
                └── day={DD}/
                    └── hour={HH}/
                        └── {batch_id}.json.gz
```

**Hive-compatible partitioning** enables zero-configuration querying with AWS Athena and Glue crawlers.

**Lifecycle rules:**
- Day 0–90: S3 Standard
- Day 90+: Glacier (cold archive)
- Day 365: Expiration

---

## Fault Tolerance & Reliability

```
                     ┌─────────────┐
                     │   Retry     │
                     │  (Lambda    │
                     │  automatic) │
                     └─────┬───────┘
                           │
Producer → Kafka → Consumer ──success──► Cassandra + S3
                         │
                         └──failure──► SQS DLQ
                                           │
                                     Manual Review /
                                     Replay Tool
```

**Retry layers:**
1. Kafka producer retries (5x with exponential backoff)
2. Lambda MSK trigger retries entire failed batch
3. DLQ holds failed messages for 14 days for manual replay

---

## Scalability

- **Lambda** auto-scales to match Kafka consumer lag (up to 1000 concurrent executions by default)
- **MSK** horizontally scalable — add broker nodes without downtime
- **Cassandra** scales reads/writes linearly with node count
- **S3** — effectively unlimited throughput with multi-prefix writes

---

## Security

- All Kafka traffic: TLS in-transit + SASL/SCRAM-512 auth
- Cassandra credentials stored in AWS Secrets Manager
- S3 buckets: AES-256 encryption, no public access
- Lambda execution role: least-privilege IAM
- VPC isolation for all compute and data resources
