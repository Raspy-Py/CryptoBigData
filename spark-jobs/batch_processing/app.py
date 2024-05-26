import time
from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum, date_format

def run_batch_job():
    spark = SparkSession.builder \
        .appName("CryptoBatchProcessor") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.cassandra.connection.port", "9042") \
        .getOrCreate()

    print("Starting the batch job...")

    keyspace = "crypto_space"
    raw_table = "raw_transactions"
    aggregated_table = "aggregated_transactions"

    end_time = datetime.now(timezone.utc)#.replace(minute=0, second=0, microsecond=0)
    start_time = end_time - timedelta(hours=1)

    print(f"Reading data from Cassandra between {start_time} and {end_time}.")

    df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table=raw_table, keyspace=keyspace) \
        .load() \
        .filter((col("timestamp") >= start_time.isoformat()) & (col("timestamp") < end_time.isoformat()))

    print(f"Number of records read: {df.count()}")

    if df.count() == 0:
        print("No data found for the given time range.")
        return

    print("Aggregating data...")

    aggregated_df = df.withColumn("hour", date_format(col("timestamp"), "yyyy-MM-dd'T'HH")) \
                      .groupBy("symbol", "hour") \
                      .agg(
                          count("*").alias("num_transactions"),
                          _sum("size").alias("total_volume")
                      )

    print("Writing aggregated data to Cassandra...")

    aggregated_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table=aggregated_table, keyspace=keyspace) \
        .save()

    print("Batch job completed.")

if __name__ == "__main__":
    while True:
        current_time = datetime.now(timezone.utc)
        next_hour = (current_time.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
        sleep_time = (next_hour - current_time).total_seconds() + 1  # just to be sure
        print(f"Sleeping for {sleep_time} seconds until the next hour.")
        time.sleep(5)
        run_batch_job()
