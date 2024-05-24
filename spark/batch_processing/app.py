import time
from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum

def run_batch_job():
    spark = SparkSession.builder \
        .appName("CryptoBatchProcessor") \
        .config("spark.mongodb.input.uri", "mongodb://mongodb:27017/crypto_db.raw_transactions") \
        .config("spark.mongodb.output.uri", "mongodb://mongodb:27017/crypto_db.aggregated_transactions") \
        .getOrCreate()

    print("Starting the batch job...")

    mongo_uri = "mongodb://mongodb:27017/crypto_db.raw_transactions"
    db_name = "crypto_db"
    collection_name = "aggregated_transactions"

    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=1)

    print(f"Reading data from MongoDB between {start_time} and {end_time}.")

    df = spark.read \
        .format("mongo") \
        .option("uri", mongo_uri) \
        .load() \
        .filter((col("timestamp") >= start_time.isoformat()) & (col("timestamp") < end_time.isoformat()))

    print(f"Number of records read: {df.count()}")

    if df.count() == 0:
        print("No data found for the given time range.")
        return

    print("Aggregating data...")

    aggregated_df = df.groupBy(
        col("symbol"),
        col("timestamp").substr(0, 13).alias("hour")  # Group by symbol and hour
    ).agg(
        count("*").alias("num_transactions"),
        _sum("size").alias("total_volume")
    )

    print("Writing aggregated data to MongoDB...")

    aggregated_df.write \
        .format("mongo") \
        .mode("append") \
        .option("uri", f"mongodb://mongodb:27017/{db_name}.{collection_name}") \
        .save()

    print("Batch job completed.")

if __name__ == "__main__":
    while True:
        current_time = datetime.now(timezone.utc)
        next_hour = (current_time.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
        sleep_time = (next_hour - current_time).total_seconds() + 1  # just to be sure
        print(f"Sleeping for {sleep_time} seconds until the next hour.")
        time.sleep(10)
        run_batch_job()
