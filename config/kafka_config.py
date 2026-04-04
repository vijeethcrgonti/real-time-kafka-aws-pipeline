# config/kafka_config.py
"""Kafka configuration management."""

from dataclasses import dataclass, field


@dataclass
class KafkaConfig:
    """Centralized Kafka configuration."""
    bootstrap_servers: str = "localhost:9092"
    default_partitions: int = 12
    replication_factor: int = 3
    compression_type: str = "gzip"
    batch_size: int = 65536          # 64KB batch
    linger_ms: int = 10             # 10ms linger for batching
    request_timeout_ms: int = 30000
    session_timeout_ms: int = 30000

    def producer_config(self) -> dict:
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "compression.type": self.compression_type,
            "batch.size": self.batch_size,
            "linger.ms": self.linger_ms,
            "request.timeout.ms": self.request_timeout_ms,
            "acks": "all",          # Wait for all replicas
            "enable.idempotence": "true",
            "max.in.flight.requests.per.connection": "5",
        }

    def consumer_config(self, group_id: str) -> dict:
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "latest",
            "enable.auto.commit": "false",
            "session.timeout.ms": self.session_timeout_ms,
            "max.poll.interval.ms": 300000,
            "fetch.min.bytes": 1024,
            "fetch.wait.max.ms": 500,
        }
