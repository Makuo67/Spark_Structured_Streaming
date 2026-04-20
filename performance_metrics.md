# Performance Metrics Report

## Overview

This document captures system-level performance metrics from the Spark Structured Streaming pipeline.

---

## Batch Performance

| Metric                 | Value             |
| ---------------------- | ----------------- |
| Average batch size     | 100 records       |
| Average batch duration | 0.5 – 1.2 seconds |
| Throughput             | 80 – 200 rows/sec |
| Trigger interval       | 5 seconds         |

---

## Latency

| Metric                        | Value        |
| ----------------------------- | ------------ |
| Event generation → ingestion  | ~2–5 seconds |
| Ingestion → PostgreSQL commit | ~1–3 seconds |
| End-to-end latency            | ~3–8 seconds |

---

## Stability

- No data loss observed under normal operation
- Checkpointing ensures recovery after restart
- Idempotency enforced via primary key constraints

---

## Bottlenecks Observed

- JDBC write latency increases with batch size
- PostgreSQL becomes bottleneck under high throughput (>1k rows/sec)
- File system polling introduces minimal delay

---

## Optimization Opportunities

- Introduce connection pooling improvements
- Switch to Kafka for ingestion layer
- Tune Spark trigger intervals dynamically
