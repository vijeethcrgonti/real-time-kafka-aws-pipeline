.PHONY: setup start stop create-topics produce consume test monitor clean

# ── Setup ────────────────────────────────────────────────────────────────────
setup:
	pip install -r requirements.txt
	docker-compose pull

# ── Kafka Local ──────────────────────────────────────────────────────────────
start:
	docker-compose up -d
	@echo "Kafka running at localhost:9092"
	@echo "Kafka UI at http://localhost:8080"
	@echo "Schema Registry at http://localhost:8081"

stop:
	docker-compose down

create-topics:
	docker exec kafka kafka-topics \
		--create --bootstrap-server localhost:9092 \
		--topic raw-events --partitions 12 --replication-factor 1 || true
	docker exec kafka kafka-topics \
		--create --bootstrap-server localhost:9092 \
		--topic validated-events --partitions 12 --replication-factor 1 || true
	docker exec kafka kafka-topics \
		--create --bootstrap-server localhost:9092 \
		--topic dlq-events --partitions 3 --replication-factor 1 || true
	@echo "Topics created: raw-events, validated-events, dlq-events"

list-topics:
	docker exec kafka kafka-topics \
		--list --bootstrap-server localhost:9092

# ── Pipeline ─────────────────────────────────────────────────────────────────
produce:
	python producer/data_producer.py \
		--rate 175 \
		--topic raw-events \
		--batch-size 500

produce-fast:
	python producer/data_producer.py \
		--rate 1000 \
		--topic raw-events \
		--duration 60

consume:
	python consumer/stream_processor.py \
		--topic raw-events \
		--output ./data/output \
		--checkpoint ./data/checkpoint

# ── Testing ──────────────────────────────────────────────────────────────────
test:
	pytest tests/ -v --cov=. --cov-report=term-missing

test-producer:
	pytest tests/test_producer.py -v

test-consumer:
	pytest tests/test_consumer.py -v

# ── Monitoring ───────────────────────────────────────────────────────────────
monitor:
	docker exec kafka kafka-consumer-groups \
		--bootstrap-server localhost:9092 \
		--describe --all-groups

lag:
	docker exec kafka kafka-consumer-groups \
		--bootstrap-server localhost:9092 \
		--describe --group stream-processor-group

# ── Cleanup ───────────────────────────────────────────────────────────────────
clean:
	docker-compose down -v
	rm -rf data/ spark-warehouse/ derby.log __pycache__ .pytest_cache
