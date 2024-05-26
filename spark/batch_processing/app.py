import time
from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum

def run_batch_job():
    spark = SparkSession.builder \
        .appName("CryptoBatchProcessor") \
        .getOrCreate()

    mongo_uri = "mongodb://mongodb:27017/crypto_db.raw_transactions"
    db_name = "crypto_db"
    collection_name = "aggregated_transactions"

    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=1)

    df = spark.read \
        .format("mongo") \
        .option("uri", mongo_uri) \
        .load() \
        .filter((col("timestamp") >= start_time.isoformat()) & (col("timestamp") < end_time.isoformat()))

    aggregated_df = df.groupBy(
        col("symbol"),
        col("timestamp").alias("timestamp")  # Group by symbol and full timestamp
    ).agg(
        count("*").alias("num_transactions"),
        _sum("size").alias("total_volume")
    )

    aggregated_df.write \
        .format("mongo") \
        .mode("append") \
        .option("uri", f"mongodb://mongodb:27017/{db_name}.{collection_name}") \
        .save()

if __name__ == "__main__":
    while True:
        current_time = datetime.utcnow()
        next_hour = (current_time.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
        sleep_time = (next_hour - current_time).total_seconds() + 1 # just to be sure
        print(f"Sleeping for {sleep_time} seconds until the next hour.")
        time.sleep(sleep_time)
        run_batch_job()
