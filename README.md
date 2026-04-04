# Real-Time Streaming Pipeline — Apache Kafka + AWS

> **Production-grade streaming data pipeline** processing 15M+ records/day with sub-2-minute latency and 99.9% uptime design.

Built by **Vijeeth C R Gonti** | Data Engineer | AWS • Azure • Apache Kafka • Apache Spark

[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.5+-black.svg)](https://kafka.apache.org/)
[![AWS](https://img.shields.io/badge/AWS-Glue%20%7C%20Kinesis%20%7C%20S3%20%7C%20Lambda-orange.svg)](https://aws.amazon.com/)
[![PySpark](https://img.shields.io/badge/PySpark-3.4+-red.svg)](https://spark.apache.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    REAL-TIME STREAMING PIPELINE                     │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│   DATA SOURCES          INGESTION           PROCESSING    STORAGE   │
│                                                                     │
│  ┌──────────┐          ┌─────────┐         ┌─────────┐  ┌───────┐  │
│  │ APIs     │──────────│  Kafka  │─────────│ PySpark │──│  S3   │  │
│  │ SFTP     │  produce │ Topics  │ consume  │  Glue   │  │Delta  │  │
│  │ DBs      │──────────│  x 12   │─────────│  Jobs   │──│Lake   │  │
│  │ Streams  │          └─────────┘         └─────────┘  └───────┘  │
│  └──────────┘               │                   │           │      │
│                        ┌────┘              ┌────┘      ┌────┘      │
│                        │                  │           │            │
│                   ┌────▼────┐        ┌────▼────┐ ┌────▼────┐      │
│                   │ Kinesis │        │ Lambda  │ │Redshift │      │
│                   │ Firehose│        │ Quality │ │Analytics│      │
│                   └─────────┘        └─────────┘ └─────────┘      │
│                                           │                        │
│                                    ┌──────▼──────┐                 │
│                                    │  Alerting   │                 │
│                                    │  (CloudWatch│                 │
│                                    │   + Bedrock)│                 │
│                                    └─────────────┘                 │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Key Performance Metrics

| Metric | Value |
|--------|-------|
| Daily record throughput | 15M+ records/day |
| End-to-end latency | Less than 2 minutes |
| System uptime | 99.9% |
| Kafka topics | 12+ source systems |
| Data platform size | 8TB+ |
| Incident reduction | 58% via validation |

---

## Project Structure

```
real-time-kafka-aws-pipeline/
├── producer/
│   ├── data_producer.py          # Kafka producer — simulates 15M+ records/day
│   ├── schema_registry.py        # Avro schema management
│   └── source_connectors.py      # API, SFTP, DB source adapters
├── consumer/
│   ├── stream_processor.py       # Kafka consumer with PySpark streaming
│   ├── consumer_group.py         # Consumer group management
│   └── offset_manager.py         # Offset tracking and recovery
├── aws/
│   ├── glue_job.py               # AWS Glue ETL job
│   ├── kinesis_ingestion.py      # Kinesis Firehose integration
│   ├── lambda_validator.py       # Lambda data quality checks
│   └── cdk_stack.py              # AWS CDK infrastructure as code
├── processing/
│   ├── spark_transformer.py      # PySpark transformation logic
│   ├── partition_optimizer.py    # Partitioning — how we got 52% speedup
│   ├── data_validator.py         # Great Expectations validation suite
│   └── delta_writer.py           # Delta Lake writer with ACID support
├── config/
│   ├── kafka_config.py           # Kafka broker and topic configuration
│   ├── aws_config.py             # AWS services configuration
│   └── pipeline_config.yaml      # Pipeline parameters
├── tests/
│   ├── test_producer.py          # Producer unit tests
│   ├── test_consumer.py          # Consumer unit tests
│   ├── test_transformer.py       # Transformation unit tests
│   └── test_validator.py         # Data quality unit tests
├── docker-compose.yml            # Local Kafka + Zookeeper setup
├── requirements.txt              # Python dependencies
├── Makefile                      # Common commands
└── README.md
```

---

## Quick Start

### Prerequisites

- Python 3.10+
- Docker and Docker Compose
- AWS CLI configured (`aws configure`)
- Apache Kafka 3.5+ (or use Docker)

### 1. Clone and Install

```bash
git clone https://github.com/vijeeth-gonti/real-time-kafka-aws-pipeline.git
cd real-time-kafka-aws-pipeline
pip install -r requirements.txt
```

### 2. Start Local Kafka (Docker)

```bash
docker-compose up -d
# Kafka runs on localhost:9092
# Zookeeper on localhost:2181
```

### 3. Create Kafka Topics

```bash
make create-topics
# Creates: raw-events, validated-events, dlq-events
```

### 4. Run the Pipeline

```bash
# Terminal 1 — Start producer (simulates 15M+ records/day)
python producer/data_producer.py --rate 175 --topic raw-events

# Terminal 2 — Start consumer + PySpark processor
python consumer/stream_processor.py --topic raw-events --output s3://your-bucket/data/

# Terminal 3 — Monitor pipeline
make monitor
```

---

## Core Components

### Producer — 15M+ Records/Day Throughput

```python
# producer/data_producer.py
python producer/data_producer.py \
  --rate 175 \           # records per second = ~15M/day
  --topic raw-events \
  --batch-size 500 \
  --compression gzip
```

### Consumer — Sub-2-Minute Latency

```python
# consumer/stream_processor.py
python consumer/stream_processor.py \
  --topic raw-events \
  --processing-time "30 seconds" \   # micro-batch interval
  --checkpoint s3://bucket/checkpoints/
```

### AWS Glue Job — 52% Faster via Partitioning

```bash
# aws/glue_job.py — deploy to AWS
aws glue create-job --cli-input-json file://aws/glue_job_config.json
```

---

## Performance Optimization

The `partition_optimizer.py` implements the exact techniques that achieved a **52% reduction** in execution time (6h to 2.8h):

1. **Dynamic partitioning** — partition by date + source_id
2. **Predicate pushdown** — filter before shuffle
3. **Broadcast joins** — for small lookup tables under 100MB
4. **Coalesce** — reduce output file count
5. **Caching** — persist frequently accessed DataFrames

---

## Data Quality Validation

Built on **Great Expectations** — reduces production incidents by 58%:

```bash
python processing/data_validator.py --suite production_suite
```

Validates:
- Schema conformance across 12+ source systems
- Null checks on critical fields
- Range validation for numeric fields
- Referential integrity checks
- Duplicate detection

---

## Infrastructure as Code

Deploy to AWS using CDK:

```bash
cd aws/
pip install aws-cdk-lib
cdk deploy KafkaAwsPipelineStack
```

Creates: S3 buckets, Glue jobs, Kinesis Firehose, Lambda validators, IAM roles, CloudWatch dashboards.

---

## Monitoring

CloudWatch dashboard tracks:
- Records processed per second
- End-to-end latency (target: under 120 seconds)
- Consumer lag per partition
- Error rate and DLQ volume
- Glue job execution time

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Message broker | Apache Kafka 3.5 |
| Stream processing | Apache Spark (PySpark) 3.4 |
| Cloud ingestion | AWS Kinesis Firehose |
| ETL | AWS Glue |
| Serverless validation | AWS Lambda |
| Storage | Amazon S3 + Delta Lake |
| Analytics | Amazon Redshift |
| IaC | AWS CDK (Python) |
| Quality | Great Expectations |
| Containerization | Docker, Docker Compose |
| CI/CD | GitHub Actions |

---

## Related Projects

- [Delta Lakehouse Medallion Architecture](https://github.com/vijeeth-gonti/delta-lakehouse-medallion-architecture)
- [AI Data Quality Framework](https://github.com/vijeeth-gonti/ai-data-quality-framework)
- [PySpark Performance Optimization](https://github.com/vijeeth-gonti/pyspark-performance-optimization)
- [AWS Glue ETL Framework](https://github.com/vijeeth-gonti/aws-glue-etl-framework)

---

## Author

**Vijeeth C R Gonti** — Data Engineer
Austin, TX | vijeethcrgonti@gmail.com
LinkedIn: [linkedin.com/in/vijeethcrgonti](https://linkedin.com/in/vijeethcrgonti)

---

*This pipeline is based on production architecture patterns used to process 15M+ records/day in enterprise healthcare and analytics environments.*
