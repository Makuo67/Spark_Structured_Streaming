DROP TABLE IF EXISTS user_events;
CREATE TABLE user_events (
    event_id TEXT PRIMARY KEY,
    event_time TIMESTAMP,
    user_id VARCHAR(50),
    product_id VARCHAR(50),
    event_type VARCHAR(20),
    price NUMERIC(10, 2),
    ingestion_time TIMESTAMP,
    processing_time TIMESTAMP
);