# User Guide — Running the Streaming Pipeline

## Prerequisites

Ensure the following are installed:

- Python 3.9+
- Apache Spark
- PostgreSQL (Docker recommended)
- pip packages:
  - pyspark
  - psycopg2-binary
  - pandas

---

## Step 1 — Start PostgreSQL (Docker)

```bash
docker run -d \
  --name postgres-streaming \
  -e POSTGRES_USER=stream_user \
  -e POSTGRES_PASSWORD=stream_pass \
  -e POSTGRES_DB=ecommerce_stream \
  -p 5432:5432 \
  postgres:15
```
