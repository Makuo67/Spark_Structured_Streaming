# Real-Time E-Commerce Event Streaming Pipeline

## Overview

This project implements a real-time data pipeline simulating user activity in an e-commerce system. It demonstrates end-to-end streaming data engineering using Apache Spark Structured Streaming and PostgreSQL as the serving layer.

---

## Architecture Summary

The system is composed of four main components:

### 1. Data Generator

- Python-based event simulator
- Produces synthetic e-commerce events (views, purchases)
- Writes partitioned CSV files continuously

### 2. Stream Ingestion Layer (Spark Structured Streaming)

- Monitors file directory for new event data
- Reads structured CSV streams
- Applies schema enforcement and transformations

### 3. Processing Layer

- Cleans and enriches incoming events
- Adds processing timestamps
- Ensures consistent schema for downstream storage

### 4. Storage Layer (PostgreSQL)

- Stores processed events in a relational schema
- Ensures idempotency via primary key constraints
- Supports upsert operations for reliability

---

## Data Flow

Data flows in the following sequence:

Data Generator → File System → Spark Streaming → PostgreSQL

---

## Key Features

- Real-time micro-batch ingestion
- Schema enforcement at ingestion layer
- Idempotent writes using UPSERT logic
- Batch-level observability and metrics
- Fault-tolerant processing via Spark checkpointing

---

## Design Philosophy

The system prioritizes:

- Reliability over raw throughput
- Data correctness over latency minimization
- Operational observability
