from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, ArrayType

# Initialize Spark session with Cassandra connection
spark = SparkSession.builder \
    .appName("CryptoStreamProcessor") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

data_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("side", StringType(), True),
    StructField("size", IntegerType(), True),
    StructField("price", FloatType(), True),
    StructField("tickDirection", StringType(), True),
    StructField("trdMatchID", StringType(), True),
    StructField("grossValue", IntegerType(), True),
    StructField("homeNotional", FloatType(), True),
    StructField("foreignNotional", FloatType(), True),
    StructField("trdType", StringType(), True)
])

schema = StructType([
    StructField("table", StringType(), True),
    StructField("action", StringType(), True),
    StructField("data", ArrayType(data_schema), True)
])

kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "crypto_data") \
    .load()

value_df = kafka_df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select(explode(col("data.data")).alias("data")) \
    .select("data.*")

def write_to_cassandra(df, epoch_id):
    df.write \
      .format("org.apache.spark.sql.cassandra") \
      .mode("append") \
      .options(table="raw_transactions", keyspace="crypto_db") \
      .save()

query = value_df.writeStream \
    .foreachBatch(write_to_cassandra) \
    .outputMode("append") \
    .start()

query.awaitTermination()
