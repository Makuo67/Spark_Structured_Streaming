# Test Cases — Streaming Pipeline Validation

## 1. Data Generator Test

| Test Case                        | Expected                       | Actual |
| -------------------------------- | ------------------------------ | ------ |
| CSV files created every interval | Files appear in /data/events   | PASS   |
| Event schema correct             | All fields present             | PASS   |
| Event types distributed          | Mostly "view", some "purchase" | PASS   |

---

## 2. Spark Streaming Test

| Test Case               | Expected                   | Actual |
| ----------------------- | -------------------------- | ------ |
| Spark detects new files | Logs show batch processing | PASS   |
| Schema enforced         | No inference errors        | PASS   |
| Batch processing stable | No crashes                 | PASS   |

---

## 3. PostgreSQL Ingestion Test

| Test Case                | Expected                        | Actual |
| ------------------------ | ------------------------------- | ------ |
| Data inserted into table | Row count increases             | PASS   |
| No duplicate event_id    | Primary key prevents duplicates | PASS   |
| Data types correct       | No type casting errors          | PASS   |

---

## 4. Failure Simulation Tests

| Test Case          | Expected                | Actual |
| ------------------ | ----------------------- | ------ |
| Stop Spark mid-run | Recovery via checkpoint | PASS   |
| Restart generator  | No duplicate corruption | PASS   |
| DB restart         | Spark retries safely    | PASS   |
