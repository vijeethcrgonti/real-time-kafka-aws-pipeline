"""
data_producer.py
================
Kafka producer simulating 15M+ records/day from multiple source systems.
Supports: API sources, SFTP-style file sources, database CDC events.

Usage:
    python producer/data_producer.py --rate 175 --topic raw-events
    python producer/data_producer.py --rate 500 --topic raw-events --source api
"""

import argparse
import json
import logging
import random
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Optional

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from config.kafka_config import KafkaConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)


# ── Event Schema ──────────────────────────────────────────────────────────────

@dataclass
class DataEvent:
    """
    Core event schema for all source systems.
    Maps to 120+ dataset schemas at CVS Health pattern.
    """
    event_id: str
    source_system: str
    record_type: str
    payload: dict
    timestamp: str
    partition_key: str
    schema_version: str = "1.0"
    checksum: Optional[str] = None

    def to_json(self) -> str:
        return json.dumps(asdict(self))


# ── Source System Simulators ──────────────────────────────────────────────────

SOURCE_SYSTEMS = [
    "pharmacy_rx", "claims_processor", "member_registry",
    "provider_network", "lab_results", "eligibility_engine",
    "billing_system", "formulary_db", "auth_service",
    "care_management", "analytics_api", "reporting_hub"
]

RECORD_TYPES = {
    "pharmacy_rx": ["prescription_fill", "refill_request", "prior_auth"],
    "claims_processor": ["claim_submitted", "claim_adjudicated", "claim_denied"],
    "member_registry": ["member_enrollment", "member_update", "member_disenroll"],
    "provider_network": ["provider_added", "provider_updated", "contract_change"],
    "lab_results": ["result_received", "result_updated", "critical_alert"],
    "eligibility_engine": ["eligibility_check", "coverage_update", "term_event"],
    "billing_system": ["invoice_created", "payment_received", "adjustment_posted"],
    "formulary_db": ["drug_added", "drug_removed", "tier_change"],
    "auth_service": ["auth_approved", "auth_denied", "auth_pending"],
    "care_management": ["case_opened", "case_updated", "case_closed"],
    "analytics_api": ["metric_captured", "report_generated", "alert_triggered"],
    "reporting_hub": ["report_requested", "report_completed", "export_ready"],
}


def generate_event(source_system: Optional[str] = None) -> DataEvent:
    """Generate a realistic data event from a source system."""
    src = source_system or random.choice(SOURCE_SYSTEMS)
    record_type = random.choice(RECORD_TYPES.get(src, ["generic_event"]))

    payload = {
        "record_id": str(uuid.uuid4()),
        "member_id": f"MBR{random.randint(100000, 999999)}",
        "value": round(random.uniform(0.01, 9999.99), 2),
        "status": random.choice(["active", "pending", "completed", "failed"]),
        "region": random.choice(["US-EAST", "US-WEST", "US-CENTRAL", "US-SOUTH"]),
        "facility_id": f"FAC{random.randint(1000, 9999)}",
        "metadata": {
            "batch_id": str(uuid.uuid4()),
            "retry_count": 0,
            "source_version": "2.1.0",
        }
    }

    return DataEvent(
        event_id=str(uuid.uuid4()),
        source_system=src,
        record_type=record_type,
        payload=payload,
        timestamp=datetime.now(timezone.utc).isoformat(),
        partition_key=f"{src}_{payload['region']}",
    )


# ── Delivery Callback ─────────────────────────────────────────────────────────

def delivery_callback(err, msg):
    """Called once per message when delivery is confirmed or fails."""
    if err:
        logger.error(f"Delivery failed | topic={msg.topic()} | error={err}")
    else:
        logger.debug(
            f"Delivered | topic={msg.topic()} "
            f"partition={msg.partition()} offset={msg.offset()}"
        )


# ── Producer Class ────────────────────────────────────────────────────────────

class DataProducer:
    """
    High-throughput Kafka producer.
    Target: 175 records/sec = 15.1M records/day.
    """

    def __init__(self, config: KafkaConfig, topic: str):
        self.topic = topic
        self.config = config
        self.producer = Producer(config.producer_config())
        self.sent_count = 0
        self.error_count = 0
        self.start_time = time.time()
        logger.info(f"Producer initialized | topic={topic} | brokers={config.bootstrap_servers}")

    def send(self, event: DataEvent) -> None:
        """Send a single event to Kafka with retry on buffer full."""
        try:
            self.producer.produce(
                topic=self.topic,
                key=event.partition_key.encode("utf-8"),
                value=event.to_json().encode("utf-8"),
                headers={
                    "source_system": event.source_system,
                    "schema_version": event.schema_version,
                    "record_type": event.record_type,
                },
                callback=delivery_callback,
            )
            self.sent_count += 1

            # Poll every 500 messages to drain delivery callbacks
            if self.sent_count % 500 == 0:
                self.producer.poll(0)

        except BufferError:
            logger.warning("Producer buffer full — waiting 0.1s and retrying")
            time.sleep(0.1)
            self.producer.poll(1)
            self.send(event)

    def run(
        self,
        rate_per_second: int = 175,
        source_system: Optional[str] = None,
        batch_size: int = 500,
        max_records: Optional[int] = None,
        duration_seconds: Optional[int] = None,
    ) -> None:
        """
        Run producer at target rate.

        Args:
            rate_per_second: Target records/sec. 175 = ~15.1M/day.
            source_system: Fixed source, or None for random rotation.
            batch_size: Records per micro-batch before sleep adjustment.
            max_records: Stop after this many records (None = run forever).
            duration_seconds: Stop after this many seconds (None = run forever).
        """
        logger.info(
            f"Starting producer | rate={rate_per_second}/s | "
            f"target={rate_per_second * 86400:,}/day"
        )

        interval = batch_size / rate_per_second  # seconds per batch
        end_time = time.time() + duration_seconds if duration_seconds else None

        try:
            while True:
                batch_start = time.time()

                # Send one batch
                for _ in range(batch_size):
                    event = generate_event(source_system)
                    self.send(event)

                    if max_records and self.sent_count >= max_records:
                        logger.info(f"Reached max_records={max_records}. Stopping.")
                        return

                # Rate limiting — sleep for remainder of interval
                elapsed = time.time() - batch_start
                sleep_time = max(0, interval - elapsed)
                if sleep_time > 0:
                    time.sleep(sleep_time)

                # Log throughput every 10k records
                if self.sent_count % 10000 == 0:
                    total_elapsed = time.time() - self.start_time
                    actual_rate = self.sent_count / total_elapsed
                    logger.info(
                        f"Sent {self.sent_count:,} records | "
                        f"rate={actual_rate:.0f}/s | "
                        f"errors={self.error_count}"
                    )

                if end_time and time.time() >= end_time:
                    logger.info(f"Duration reached. Stopping after {duration_seconds}s.")
                    return

        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        finally:
            self.flush()

    def flush(self) -> None:
        """Flush remaining messages and log final stats."""
        logger.info("Flushing producer buffer...")
        remaining = self.producer.flush(timeout=30)
        total_elapsed = time.time() - self.start_time
        avg_rate = self.sent_count / total_elapsed if total_elapsed > 0 else 0

        logger.info(
            f"\n{'='*50}\n"
            f"Producer Summary\n"
            f"  Total sent    : {self.sent_count:,}\n"
            f"  Errors        : {self.error_count:,}\n"
            f"  Duration      : {total_elapsed:.1f}s\n"
            f"  Avg rate      : {avg_rate:.0f} records/sec\n"
            f"  Daily proj.   : {avg_rate * 86400:,.0f} records/day\n"
            f"  Unflushed     : {remaining}\n"
            f"{'='*50}"
        )


# ── Topic Management ──────────────────────────────────────────────────────────

def ensure_topics_exist(config: KafkaConfig, topics: list[str]) -> None:
    """Create Kafka topics if they don't already exist."""
    admin = AdminClient({"bootstrap.servers": config.bootstrap_servers})
    existing = set(admin.list_topics(timeout=10).topics.keys())

    new_topics = [
        NewTopic(
            topic=t,
            num_partitions=config.default_partitions,
            replication_factor=config.replication_factor,
        )
        for t in topics
        if t not in existing
    ]

    if not new_topics:
        logger.info("All topics already exist")
        return

    results = admin.create_topics(new_topics)
    for topic, future in results.items():
        try:
            future.result()
            logger.info(f"Created topic: {topic}")
        except Exception as e:
            logger.error(f"Failed to create topic {topic}: {e}")


# ── CLI Entrypoint ────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(description="Kafka Data Producer")
    parser.add_argument("--brokers", default="localhost:9092", help="Kafka brokers")
    parser.add_argument("--topic", default="raw-events", help="Target Kafka topic")
    parser.add_argument("--rate", type=int, default=175, help="Records per second (175=15M/day)")
    parser.add_argument("--batch-size", type=int, default=500, help="Records per batch")
    parser.add_argument("--source", default=None, help="Fixed source system (default=random)")
    parser.add_argument("--max-records", type=int, default=None, help="Stop after N records")
    parser.add_argument("--duration", type=int, default=None, help="Stop after N seconds")
    parser.add_argument("--create-topics", action="store_true", help="Create topics before producing")
    return parser.parse_args()


def main():
    args = parse_args()
    config = KafkaConfig(bootstrap_servers=args.brokers)

    if args.create_topics:
        ensure_topics_exist(config, ["raw-events", "validated-events", "dlq-events"])

    producer = DataProducer(config=config, topic=args.topic)
    producer.run(
        rate_per_second=args.rate,
        source_system=args.source,
        batch_size=args.batch_size,
        max_records=args.max_records,
        duration_seconds=args.duration,
    )


if __name__ == "__main__":
    main()
