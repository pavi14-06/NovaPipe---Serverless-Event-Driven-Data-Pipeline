"""
NovaPipe — Kafka Topic Manager
Creates, configures, and manages Kafka topics for the pipeline.
"""

import json
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Optional

from kafka.admin import KafkaAdminClient, NewTopic, ConfigResource, ConfigResourceType
from kafka.errors import TopicAlreadyExistsError, KafkaError

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ─── Config ────────────────────────────────────────────────────────────────────

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
PIPELINE_ENV      = os.environ.get("PIPELINE_ENV", "development")


@dataclass
class TopicSpec:
    name:              str
    num_partitions:    int   = 6
    replication_factor: int  = 3
    retention_ms:      int   = 604_800_000   # 7 days
    cleanup_policy:    str   = "delete"
    compression_type:  str   = "gzip"
    max_message_bytes: int   = 1_048_576     # 1 MB
    min_insync_replicas: int = 2
    description:       str   = ""


# ─── Topic Definitions ────────────────────────────────────────────────────────

TOPICS: list[TopicSpec] = [
    TopicSpec(
        name="novapipe.events",
        num_partitions=12,
        replication_factor=3,
        retention_ms=604_800_000,      # 7 days hot retention
        description="Primary event ingestion topic",
    ),
    TopicSpec(
        name="novapipe.events.transformed",
        num_partitions=12,
        replication_factor=3,
        retention_ms=259_200_000,      # 3 days
        description="Enriched & validated events",
    ),
    TopicSpec(
        name="novapipe.events.dlq",
        num_partitions=3,
        replication_factor=3,
        retention_ms=2_592_000_000,    # 30 days for DLQ
        cleanup_policy="compact",      # Keep last failure per key
        description="Dead Letter Queue — failed events",
    ),
    TopicSpec(
        name="novapipe.metrics",
        num_partitions=6,
        replication_factor=3,
        retention_ms=86_400_000,       # 1 day
        description="Pipeline telemetry and metrics",
    ),
    TopicSpec(
        name="novapipe.audit",
        num_partitions=3,
        replication_factor=3,
        retention_ms=7_776_000_000,    # 90 days
        cleanup_policy="compact",
        description="Audit log — immutable event trail",
    ),
]


# ─── Manager ──────────────────────────────────────────────────────────────────

class KafkaTopicManager:
    def __init__(self, bootstrap_servers: list[str]) -> None:
        self._servers = bootstrap_servers
        self._admin: Optional[KafkaAdminClient] = None

    def _get_admin(self) -> KafkaAdminClient:
        if self._admin is None:
            self._admin = KafkaAdminClient(
                bootstrap_servers=self._servers,
                client_id="novapipe-topic-manager",
                request_timeout_ms=30_000,
            )
            logger.info("Connected to Kafka brokers: %s", self._servers)
        return self._admin

    def _topic_config(self, spec: TopicSpec) -> dict[str, str]:
        return {
            "retention.ms":        str(spec.retention_ms),
            "cleanup.policy":      spec.cleanup_policy,
            "compression.type":    spec.compression_type,
            "max.message.bytes":   str(spec.max_message_bytes),
            "min.insync.replicas": str(spec.min_insync_replicas),
        }

    def list_topics(self) -> list[str]:
        admin = self._get_admin()
        metadata = admin.list_topics()
        return sorted(metadata)

    def topic_exists(self, name: str) -> bool:
        return name in self.list_topics()

    def create_topic(self, spec: TopicSpec, skip_if_exists: bool = True) -> bool:
        if skip_if_exists and self.topic_exists(spec.name):
            logger.info("Topic already exists, skipping: %s", spec.name)
            return False

        new_topic = NewTopic(
            name=spec.name,
            num_partitions=spec.num_partitions,
            replication_factor=spec.replication_factor,
            topic_configs=self._topic_config(spec),
        )

        try:
            self._get_admin().create_topics([new_topic], validate_only=False)
            logger.info(
                "✅ Created topic: %s | partitions=%d replication=%d",
                spec.name, spec.num_partitions, spec.replication_factor,
            )
            return True
        except TopicAlreadyExistsError:
            logger.info("Topic already exists: %s", spec.name)
            return False
        except KafkaError as exc:
            logger.error("Failed to create topic %s: %s", spec.name, exc)
            raise

    def create_all_topics(self) -> dict[str, bool]:
        results = {}
        for spec in TOPICS:
            try:
                results[spec.name] = self.create_topic(spec)
            except Exception as exc:
                logger.error("Error creating topic %s: %s", spec.name, exc)
                results[spec.name] = False
        return results

    def delete_topic(self, name: str) -> bool:
        try:
            self._get_admin().delete_topics([name])
            logger.info("🗑️  Deleted topic: %s", name)
            return True
        except KafkaError as exc:
            logger.error("Failed to delete topic %s: %s", name, exc)
            return False

    def describe_topic(self, name: str) -> dict:
        try:
            configs = self._get_admin().describe_configs([
                ConfigResource(ConfigResourceType.TOPIC, name)
            ])
            return {entry.name: entry.value for config in configs for entry in config.resources[0].config_entries}
        except Exception as exc:
            logger.error("Failed to describe topic %s: %s", name, exc)
            return {}

    def close(self) -> None:
        if self._admin:
            self._admin.close()


# ─── CLI Entry Point ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="NovaPipe Kafka Topic Manager")
    parser.add_argument("action", choices=["create", "list", "delete", "describe"])
    parser.add_argument("--topic", help="Topic name (for delete/describe)")
    args = parser.parse_args()

    manager = KafkaTopicManager(BOOTSTRAP_SERVERS)

    try:
        if args.action == "create":
            results = manager.create_all_topics()
            print(json.dumps(results, indent=2))

        elif args.action == "list":
            topics = manager.list_topics()
            for t in topics:
                print(t)

        elif args.action == "delete":
            if not args.topic:
                parser.error("--topic required for delete")
            ok = manager.delete_topic(args.topic)
            print("Deleted" if ok else "Failed")

        elif args.action == "describe":
            if not args.topic:
                parser.error("--topic required for describe")
            config = manager.describe_topic(args.topic)
            print(json.dumps(config, indent=2))

    finally:
        manager.close()
