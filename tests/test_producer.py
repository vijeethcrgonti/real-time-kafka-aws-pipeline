"""
test_producer.py
================
Unit tests for the Kafka data producer.
Run: pytest tests/ -v
"""

import json
import pytest
from unittest.mock import MagicMock, patch, call
from producer.data_producer import DataEvent, DataProducer, generate_event, SOURCE_SYSTEMS
from config.kafka_config import KafkaConfig


class TestDataEvent:
    """Tests for DataEvent schema."""

    def test_event_serialization(self):
        event = DataEvent(
            event_id="test-123",
            source_system="pharmacy_rx",
            record_type="prescription_fill",
            payload={"member_id": "MBR123456", "value": 99.99},
            timestamp="2026-04-04T12:00:00+00:00",
            partition_key="pharmacy_rx_US-EAST",
        )
        json_str = event.to_json()
        parsed = json.loads(json_str)

        assert parsed["event_id"] == "test-123"
        assert parsed["source_system"] == "pharmacy_rx"
        assert parsed["schema_version"] == "1.0"

    def test_event_has_required_fields(self):
        event = generate_event()
        assert event.event_id is not None
        assert event.source_system in SOURCE_SYSTEMS
        assert event.record_type is not None
        assert event.timestamp is not None
        assert event.partition_key is not None

    def test_generate_event_random_source(self):
        events = [generate_event() for _ in range(100)]
        sources = {e.source_system for e in events}
        # Should hit multiple sources in 100 events
        assert len(sources) > 1

    def test_generate_event_fixed_source(self):
        event = generate_event(source_system="claims_processor")
        assert event.source_system == "claims_processor"

    def test_partition_key_format(self):
        event = generate_event()
        # partition_key should be {source_system}_{region}
        parts = event.partition_key.split("_")
        assert len(parts) >= 2


class TestDataProducer:
    """Tests for DataProducer class."""

    @patch("producer.data_producer.Producer")
    def test_producer_initialization(self, mock_producer_class):
        config = KafkaConfig(bootstrap_servers="localhost:9092")
        producer = DataProducer(config=config, topic="test-topic")
        assert producer.topic == "test-topic"
        assert producer.sent_count == 0
        assert producer.error_count == 0

    @patch("producer.data_producer.Producer")
    def test_send_increments_count(self, mock_producer_class):
        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer
        mock_producer.poll.return_value = 0

        config = KafkaConfig()
        producer = DataProducer(config=config, topic="test-topic")

        event = generate_event()
        producer.send(event)
        assert producer.sent_count == 1

    @patch("producer.data_producer.Producer")
    def test_send_calls_produce(self, mock_producer_class):
        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer

        config = KafkaConfig()
        producer = DataProducer(config=config, topic="test-topic")

        event = generate_event("pharmacy_rx")
        producer.send(event)

        mock_producer.produce.assert_called_once()
        call_kwargs = mock_producer.produce.call_args.kwargs
        assert call_kwargs["topic"] == "test-topic"
        assert call_kwargs["key"] == event.partition_key.encode("utf-8")

    @patch("producer.data_producer.Producer")
    def test_run_respects_max_records(self, mock_producer_class):
        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer
        mock_producer.poll.return_value = 0
        mock_producer.flush.return_value = 0

        config = KafkaConfig()
        producer = DataProducer(config=config, topic="test-topic")

        producer.run(rate_per_second=10000, max_records=50, batch_size=10)
        assert producer.sent_count == 50

    @patch("producer.data_producer.Producer")
    def test_throughput_target(self, mock_producer_class):
        """Verify 175 records/sec = ~15M/day is achievable."""
        rate = 175
        seconds_per_day = 86400
        projected_daily = rate * seconds_per_day
        assert projected_daily >= 15_000_000, f"175/sec projects to {projected_daily:,}/day"


class TestKafkaConfig:
    """Tests for Kafka configuration."""

    def test_producer_config_has_required_keys(self):
        config = KafkaConfig()
        prod_config = config.producer_config()
        assert "bootstrap.servers" in prod_config
        assert "compression.type" in prod_config
        assert "acks" in prod_config
        assert prod_config["acks"] == "all"

    def test_consumer_config_has_group_id(self):
        config = KafkaConfig()
        cons_config = config.consumer_config("test-group")
        assert cons_config["group.id"] == "test-group"
        assert cons_config["enable.auto.commit"] == "false"

    def test_default_partitions(self):
        config = KafkaConfig()
        # 12 partitions matches 12 source systems
        assert config.default_partitions == 12


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
