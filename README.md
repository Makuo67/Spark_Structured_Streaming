# Spark Structured Streaming E-Commerce Pipeline

## Overview

This project implements a fault-tolerant, real-time data pipeline for processing e-commerce user events using Apache Spark Structured Streaming with PostgreSQL as the persistent storage layer. The system simulates continuous event generation, stream ingestion, transformation, and exactly-once delivery to a relational database.

Key objectives:

- Demonstrate production-grade streaming patterns
- Ensure data integrity through schema enforcement and idempotent writes
- Provide end-to-end observability and fault recovery
- Support scalable micro-batch processing

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│   Data          │     │   Spark          │     │   PostgreSQL     │
│   Generator     │───▶ │ Structured       │───▶ │                  │
│ (generator.py)  │     │ Streaming        │     │ • user_events    │
└─────────────────┘     │ (main app)       │     │ • user_events_   │
                        └──────────────────┘     │   staging        │
                                                 └──────────────────┘
```

### Components

1. **Event Generator** (`generator.py`)
   - Produces synthetic view/purchase events (80/20 ratio)
   - Batch size: 100 events every 2 seconds
   - Partitioned Parquet-like output: `data/events/date=YYYY-MM-DD/hour=HH/events_*.csv`

2. **Streaming Processor** (`spark_streaming_to_postgres.py`)
   - File source stream with strict schema and watermarking
   - Transformations: cleaning, timestamp enrichment
   - Sink: foreachBatch with JDBC staging + UPSERT merge
   - Checkpointing for fault tolerance
   - Trigger interval: 5 seconds

3. **Database Schema** (`postgres_setup.sql`)
   - `user_events`: Deduplicated events with primary key `event_id`
   - `user_events_staging`: Atomic batch landing zone

## Features

- **Exactly-Once Semantics**: Primary key constraints + UPSERT logic
- **Schema Enforcement**: Strict Spark schema at ingestion
- **Fault Tolerance**: Checkpointing + connection pooling
- **Observability**: Structured logging to `logs/` directory
- **Scalability**: File-based partitioning + configurable batch sizes

## Prerequisites

- Python 3.9+
- Apache Spark 3.x (with `pyspark`)
- PostgreSQL 15+
- Required Python packages:
  ```
  pip install pyspark psycopg2-binary pandas
  ```

## Quick Start

### 1. Start PostgreSQL

```bash
docker run -d \
  --name ecommerce-postgres \
  -e POSTGRES_USER=stream_user \
  -e POSTGRES_PASSWORD=stream_pass \
  -e POSTGRES_DB=ecommerce_stream \
  -p 5432:5432 \
  postgres:15
```

### 2. Initialize Database

```bash
docker exec -i ecommerce-postgres psql -U stream_user -d ecommerce_stream < postgres_setup.sql
```

### 3. Start Data Generator (Terminal 1)

```bash
cd Spark_Structured_Streaming
python generator.py
```

### 4. Start Streaming Pipeline (Terminal 2)

```bash
spark-submit spark_streaming_to_postgres.py
```

## Configuration

Database parameters are defined in `spark_streaming_to_postgres.py`:

```python
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'ecommerce_stream',
    'user': 'stream_user',
    'password': 'stream_pass'
}
```

Tune via environment variables or external config as needed.

## Event Schema

| Field             | Type        | Description                   |
| ----------------- | ----------- | ----------------------------- |
| `event_id`        | STRING (PK) | Unique event identifier       |
| `event_time`      | TIMESTAMP   | When event occurred           |
| `user_id`         | STRING      | `user_N` (N=1..1000)          |
| `product_id`      | STRING      | `product_N` (N=1..500)        |
| `event_type`      | STRING      | `view` or `purchase`          |
| `price`           | DOUBLE      | Purchase amount (0 for views) |
| `ingestion_time`  | TIMESTAMP   | File write time               |
| `processing_time` | TIMESTAMP   | Spark processing time         |

## Monitoring and Logs

- Generator logs: `logs/data_generator.log`
- Spark logs: `logs/spark_streaming.log`
- Metrics: `logs/metrics.log`
- Checkpoints: `checkpoints/events_pipeline/`

Tail logs for real-time observability:

```bash
tail -f logs/*.log
```

## Performance Considerations

- **Throughput**: ~1800 events/minute (configurable)
- **Latency**: End-to-end <10s (micro-batch)
- **Resource Usage**: Single-node Spark acceptable for demo
- Scale by increasing Spark executors/partitions

## Testing and Validation

1. Verify data generation: `ls -la data/events/`
2. Check stream progress: Spark UI (4040)
3. Query results:
   ```sql
   SELECT event_type, COUNT(*) FROM user_events GROUP BY event_type;
   ```

## Directory Structure

```
Spark_Structured_Streaming/
├── generator.py                 # Event producer
├── spark_streaming_to_postgres.py # Streaming processor
├── postgres_setup.sql           # Database schema
├── data/events/                 # Input stream (auto-created)
├── checkpoints/                 # Fault recovery
├── logs/                        # Observability
├── project_overview.md          # Architecture details
├── user_guide.md                # Deployment guide
└── README.md                    # This file
```

## Next Steps

- Integrate Kafka for true streaming source
- Add aggregation state stores (windowed metrics)
- Implement alerting on processing lag
- Containerize with Docker Compose

## License

Apache 2.0
