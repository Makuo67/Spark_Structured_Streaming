import logging
from psycopg2 import pool
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, current_timestamp

# ==============================
# CONFIGURATION
# ==============================
INPUT_PATH = "data/events"
CHECKPOINT_PATH = "checkpoints/events_pipeline"

# ==============================
# LOGGING SETUP
# ==============================
logging.basicConfig(
    filename="logs/spark_streaming.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
metrics_logger = logging.getLogger("metrics")
metrics_logger.setLevel(logging.INFO)

metrics_handler = logging.FileHandler("logs/metrics.log")
metrics_handler.setFormatter(logging.Formatter("%(message)s"))

metrics_logger.addHandler(metrics_handler)

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "ecommerce_stream",
    "user": "stream_user",
    "password": "stream_pass"
}

JDBC_URL = "jdbc:postgresql://localhost:5432/ecommerce_stream"
JDBC_PROPERTIES = {
    "user": "stream_user",
    "password": "stream_pass",
    "driver": "org.postgresql.Driver"
}

db_pool = pool.SimpleConnectionPool(
    1, 10,
    host="localhost",
    port=5432,
    database="ecommerce_stream",
    user="stream_user",
    password="stream_pass"
)

logger = logging.getLogger(__name__)

# ==============================
# SPARK SESSION
# ==============================
spark = SparkSession.builder \
    .appName("EcommerceStreamingPipeline") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==============================
# SCHEMA DEFINITION (STRICT)
# ==============================
schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_time", TimestampType(), False),
    StructField("user_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("price", DoubleType(), False),
    StructField("ingestion_time", TimestampType(), False),
])

# ==============================
# READ STREAM
# ==============================
df_raw = spark.readStream \
    .schema(schema) \
    .option("header", True) \
    .option("maxFilesPerTrigger", 1) \
    .csv(INPUT_PATH)

# ==============================
# TRANSFORMATIONS
# ==============================
df_clean = df_raw \
    .withColumn("processing_time", current_timestamp()) \
    .filter(col("event_id").isNotNull())

# watermark (future-proofing)
df_clean = df_clean.withWatermark("event_time", "5 minutes")

# ==============================
# FOREACH BATCH LOGIC
# ==============================


def process_batch(batch_df, batch_id):

    if batch_df.rdd.isEmpty():
        return

    logger.info(f"Batch {batch_id} started")

    start = time.time()

    try:
        clean_df = batch_df.select(
            "event_id",
            "event_time",
            "user_id",
            "product_id",
            "event_type",
            "price",
            "ingestion_time",
            "processing_time"
        )

        # STEP 1: write to staging (Spark handles bulk insert)
        clean_df.write \
            .format("jdbc") \
            .option("url", JDBC_URL) \
            .option("dbtable", "user_events_staging") \
            .options(**JDBC_PROPERTIES) \
            .mode("append") \
            .save()

        # STEP 2: UPSERT MERGE (atomic operation)
        conn = db_pool.getconn()
        conn.autocommit = False
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO user_events
            SELECT * FROM user_events_staging
            ON CONFLICT (event_id)
            DO UPDATE SET
                event_time = EXCLUDED.event_time,
                user_id = EXCLUDED.user_id,
                product_id = EXCLUDED.product_id,
                event_type = EXCLUDED.event_type,
                price = EXCLUDED.price,
                ingestion_time = EXCLUDED.ingestion_time,
                processing_time = EXCLUDED.processing_time;
        """)

        # STEP 3: clean staging
        cur.execute("TRUNCATE TABLE user_events_staging;")

        conn.commit()
        cur.close()
        db_pool.putconn(conn)

        duration = time.time() - start

        logger.info(
            f"Batch {batch_id} SUCCESS | "
            f"records={batch_df.count()} | "
            f"time={round(duration, 3)}s"
        )

    except Exception as e:
        logger.error(f"Batch {batch_id} FAILED: {e}", exc_info=True)
        raise


# ==============================
# WRITE STREAM
# ==============================
query = df_clean.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .trigger(processingTime="5 seconds") \
    .start()
logger.info("Streaming query started.")

query.awaitTermination()
