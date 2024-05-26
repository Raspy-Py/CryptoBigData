import time
from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum

def run_batch_job(run_interval):
    spark = SparkSession.builder \
        .appName("CryptoBatchProcessor") \
        .getOrCreate()

    mongo_uri = "mongodb://mongodb:27017/crypto_db.raw_transactions"
    db_name = "crypto_db"
    collection_name = "aggregated_transactions"

    end_time = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    start_time = end_time - timedelta(minutes=run_interval)

    print(f"Processing data from {start_time} to {end_time}.")

    df = spark.read \
        .format("mongo") \
        .option("uri", mongo_uri) \
        .load() \
        .filter((col("timestamp") >= start_time.isoformat()) & (col("timestamp") < end_time.isoformat()))

    if df.count() == 0:
        print("No data found for the given time range.")
        return

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

    print("Batch job completed.")

if __name__ == "__main__":
    run_interval = 5  # Interval in minutes
    while True:
        current_time = datetime.now(timezone.utc)
        next_run_time = (current_time.replace(second=0, microsecond=0) + timedelta(minutes=run_interval))
        sleep_time = (next_run_time - current_time).total_seconds()
        print(f"Sleeping for {sleep_time} seconds until the next interval.")
        time.sleep(sleep_time)
        run_batch_job(run_interval)
