import os
import time
import uuid
import random
import logging
from datetime import datetime
import pandas as pd

# ==============================
# CONFIGURATION
# ==============================
OUTPUT_BASE_DIR = "data/events"
BATCH_SIZE = 100
SLEEP_INTERVAL = 2  # seconds
VIEW_PROBABILITY = 0.8

# ==============================
# LOGGING SETUP
# ==============================
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename="logs/data_generator.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger(__name__)

# ==============================
# DATA GENERATION FUNCTIONS
# ==============================


def generate_event():
    event_type = "view" if random.random() < VIEW_PROBABILITY else "purchase"

    price = round(random.uniform(5, 500),
                  2) if event_type == "purchase" else 0.0

    return {
        "event_id": str(uuid.uuid4()),
        "event_time": datetime.utcnow(),
        "user_id": f"user_{random.randint(1, 1000)}",
        "product_id": f"product_{random.randint(1, 500)}",
        "event_type": event_type,
        "price": price,
        "ingestion_time": datetime.utcnow()
    }


def generate_batch(batch_size):
    return [generate_event() for _ in range(batch_size)]

# ==============================
# FILE WRITING
# ==============================


def get_partition_path(base_dir):
    now = datetime.utcnow()
    path = os.path.join(
        base_dir,
        f"date={now.strftime('%Y-%m-%d')}",
        f"hour={now.strftime('%H')}"
    )
    os.makedirs(path, exist_ok=True)
    return path


def write_batch_to_csv(batch):
    df = pd.DataFrame(batch)

    partition_path = get_partition_path(OUTPUT_BASE_DIR)

    file_name = f"events_{int(time.time() * 1000)}.csv"
    full_path = os.path.join(partition_path, file_name)

    df.to_csv(full_path, index=False)

    logger.info(f"Wrote batch: {full_path} ({len(df)} records)")

# ==============================
# MAIN LOOP
# ==============================


def run():
    logger.info("Starting data generator...")

    try:
        while True:
            batch = generate_batch(BATCH_SIZE)
            write_batch_to_csv(batch)
            time.sleep(SLEEP_INTERVAL)

    except KeyboardInterrupt:
        logger.info("Data generator stopped manually.")

    except Exception as e:
        logger.error(f"Error occurred: {e}", exc_info=True)


if __name__ == "__main__":
    run()
