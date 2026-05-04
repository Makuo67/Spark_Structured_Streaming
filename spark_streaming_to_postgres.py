import logging
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import *

# ==============================
# CONFIGURATION
# ==============================
INPUT_PATH = "data/events"
CHECKPOINT_PATH = "checkpoints/events_pipeline"

JDBC_URL = "jdbc:postgresql://localhost:5432/ecommerce_stream"

JDBC_PROPERTIES = {
    "user": "stream_user",
    "password": "stream_pass",
    "driver": "org.postgresql.Driver"
}

# ==============================
# LOGGING
# ==============================
logging.basicConfig(
    filename="logs/spark_streaming.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
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
# SCHEMA
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
# TRANSFORMATION
# ==============================
df_clean = df_raw \
    .withWatermark("event_time", "5 minutes") \
    .withColumn("processing_time", current_timestamp()) \
    .filter(col("event_id").isNotNull())

# ==============================
# FOREACH BATCH
# ==============================
def process_batch(batch_df, batch_id):

    if batch_df.rdd.isEmpty():
        return

    start = time.time()
    logger.info(f"Batch {batch_id} started")

    try:
        # Ensure correct Spark types
        final_df = batch_df.select(
            col("event_id").cast("string"),
            "event_time",
            "user_id",
            "product_id",
            "event_type",
            "price",
            "ingestion_time",
            "processing_time"
        )

        # Write directly to Postgres using JDBC
        final_df.write \
            .format("jdbc") \
            .option("url", JDBC_URL) \
            .option("dbtable", "public.user_events") \
            .options(**JDBC_PROPERTIES) \
            .mode("append") \
            .save()

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
# STREAM
# ==============================
query = df_clean.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .trigger(processingTime="5 seconds") \
    .start()

logger.info("Streaming started...")

query.awaitTermination()