"""
NovaPipe — Cassandra Client Wrapper
Thread-safe, lazy-initialized Cassandra session with
connection pooling, retry logic, and metrics instrumentation.
"""

from __future__ import annotations

import json
import logging
import os
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from threading import Lock
from typing import Any, Generator, Optional

from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT, ResponseFuture
from cassandra.policies import (
    DCAwareRoundRobinPolicy,
    ExponentialReconnectionPolicy,
    RetryPolicy,
    TokenAwarePolicy,
)
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement, PreparedStatement, ConsistencyLevel

logger = logging.getLogger(__name__)


# ─── Config ────────────────────────────────────────────────────────────────────

@dataclass
class CassandraConfig:
    contact_points: list[str]
    port:           int   = 9042
    keyspace:       str   = "novapipe"
    local_dc:       str   = "us-east-1"
    username:       Optional[str] = None
    password:       Optional[str] = None
    request_timeout: float = 10.0
    connect_timeout: float = 15.0
    max_connections: int   = 10

    @classmethod
    def from_env(cls) -> "CassandraConfig":
        return cls(
            contact_points=os.environ["CASSANDRA_CONTACT_POINTS"].split(","),
            port=int(os.environ.get("CASSANDRA_PORT", "9042")),
            keyspace=os.environ.get("CASSANDRA_KEYSPACE", "novapipe"),
            local_dc=os.environ.get("CASSANDRA_LOCAL_DC", "us-east-1"),
            username=os.environ.get("CASSANDRA_USERNAME"),
            password=os.environ.get("CASSANDRA_PASSWORD"),
        )


# ─── Metrics ───────────────────────────────────────────────────────────────────

@dataclass
class QueryMetrics:
    total_queries:   int   = 0
    total_errors:    int   = 0
    total_latency_ms: float = 0.0
    _lock: Lock = field(default_factory=Lock, repr=False)

    def record(self, latency_ms: float, error: bool = False) -> None:
        with self._lock:
            self.total_queries += 1
            self.total_latency_ms += latency_ms
            if error:
                self.total_errors += 1

    @property
    def avg_latency_ms(self) -> float:
        return self.total_latency_ms / self.total_queries if self.total_queries else 0.0

    @property
    def error_rate(self) -> float:
        return self.total_errors / self.total_queries if self.total_queries else 0.0

    def report(self) -> dict:
        return {
            "total_queries":    self.total_queries,
            "total_errors":     self.total_errors,
            "avg_latency_ms":   round(self.avg_latency_ms, 2),
            "error_rate_pct":   round(self.error_rate * 100, 2),
        }


# ─── Client ────────────────────────────────────────────────────────────────────

class CassandraClient:
    """
    Thread-safe Cassandra client with:
    - Lazy connection initialization
    - Prepared statement caching
    - Automatic reconnection
    - Query metrics
    """

    def __init__(self, config: CassandraConfig) -> None:
        self._config   = config
        self._cluster  = None
        self._session  = None
        self._lock     = Lock()
        self._stmts: dict[str, PreparedStatement] = {}
        self.metrics   = QueryMetrics()

    # ── Connection ──────────────────────────────────────────────────────────

    def _build_cluster(self) -> Cluster:
        auth = None
        if self._config.username:
            auth = PlainTextAuthProvider(
                username=self._config.username,
                password=self._config.password,
            )

        profile = ExecutionProfile(
            load_balancing_policy=TokenAwarePolicy(
                DCAwareRoundRobinPolicy(local_dc=self._config.local_dc)
            ),
            retry_policy=RetryPolicy(),
            consistency_level=ConsistencyLevel.LOCAL_QUORUM,
            request_timeout=self._config.request_timeout,
        )

        return Cluster(
            contact_points=self._config.contact_points,
            port=self._config.port,
            auth_provider=auth,
            execution_profiles={EXEC_PROFILE_DEFAULT: profile},
            reconnection_policy=ExponentialReconnectionPolicy(
                base_delay=1.0,
                max_delay=60.0,
            ),
            connect_timeout=self._config.connect_timeout,
            protocol_version=4,
        )

    @property
    def session(self):
        if self._session is None or self._session.is_shutdown:
            with self._lock:
                if self._session is None or self._session.is_shutdown:
                    logger.info("Opening Cassandra connection → %s", self._config.contact_points)
                    self._cluster = self._build_cluster()
                    self._session = self._cluster.connect(self._config.keyspace)
                    logger.info("Cassandra connected → keyspace=%s", self._config.keyspace)
        return self._session

    def close(self) -> None:
        if self._cluster:
            self._cluster.shutdown()
            logger.info("Cassandra connection closed")

    # ── Prepared Statements ─────────────────────────────────────────────────

    def prepare(self, cql: str) -> PreparedStatement:
        """Cache prepared statements by CQL string."""
        if cql not in self._stmts:
            self._stmts[cql] = self.session.prepare(cql)
        return self._stmts[cql]

    # ── Execute ─────────────────────────────────────────────────────────────

    def execute(
        self,
        statement: str | PreparedStatement | SimpleStatement,
        parameters: tuple | list | None = None,
        *,
        timeout: float | None = None,
    ) -> Any:
        """Execute a CQL statement and record metrics."""
        start = time.monotonic()
        error = False
        try:
            if isinstance(statement, str):
                statement = SimpleStatement(statement)
            result = self.session.execute(statement, parameters, timeout=timeout)
            return result
        except Exception:
            error = True
            raise
        finally:
            latency_ms = (time.monotonic() - start) * 1000
            self.metrics.record(latency_ms, error=error)

    def execute_async(
        self,
        statement: str | PreparedStatement,
        parameters: tuple | list | None = None,
    ) -> ResponseFuture:
        """Non-blocking async execution (fire-and-forget pattern)."""
        if isinstance(statement, str):
            statement = self.prepare(statement)
        return self.session.execute_async(statement, parameters)

    # ── Convenience Methods ─────────────────────────────────────────────────

    def insert_event(self, envelope: dict) -> bool:
        """Insert a single event envelope into the events table."""
        cql = self.prepare("""
            INSERT INTO events (
                source, year_month, event_timestamp, event_id,
                pipeline, epoch_ms, payload, schema_version,
                ingested_at, payload_bytes, payload_keys,
                payload_fingerprint, processed_at, processed_by
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)
        ts = datetime.fromisoformat(envelope["timestamp"])
        try:
            self.execute(cql, (
                envelope["source"],
                ts.strftime("%Y-%m"),
                ts,
                envelope["event_id"],
                envelope.get("pipeline", ""),
                envelope.get("epoch_ms", 0),
                json.dumps(envelope.get("payload", {})),
                envelope.get("schema_version", "1.0"),
                datetime.now(timezone.utc),
                envelope.get("payload_bytes", 0),
                envelope.get("payload_keys", []),
                envelope.get("payload_fingerprint", ""),
                datetime.fromisoformat(envelope["processed_at"]) if envelope.get("processed_at") else None,
                envelope.get("processed_by", ""),
            ))
            return True
        except Exception as exc:
            logger.error("insert_event failed: %s", exc)
            return False

    def is_duplicate(self, event_id: str) -> bool:
        """Check deduplication table. Returns True if already seen."""
        cql = "SELECT event_id FROM event_dedup WHERE event_id = ?"
        stmt = self.prepare(cql)
        rows = list(self.execute(stmt, (event_id,)))
        return len(rows) > 0

    def mark_seen(self, event_id: str, source: str) -> None:
        """Insert into deduplication table (TTL=24h set at schema level)."""
        cql = "INSERT INTO event_dedup (event_id, seen_at, source) VALUES (?, ?, ?)"
        stmt = self.prepare(cql)
        self.execute(stmt, (event_id, datetime.now(timezone.utc), source))

    def get_metrics(self) -> dict:
        return self.metrics.report()


# ─── Singleton Factory ─────────────────────────────────────────────────────────

_default_client: CassandraClient | None = None
_client_lock = Lock()


def get_client(config: CassandraConfig | None = None) -> CassandraClient:
    """Return the module-level singleton CassandraClient."""
    global _default_client
    if _default_client is None:
        with _client_lock:
            if _default_client is None:
                cfg = config or CassandraConfig.from_env()
                _default_client = CassandraClient(cfg)
    return _default_client
