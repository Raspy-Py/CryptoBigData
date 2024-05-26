from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, ArrayType

spark = SparkSession.builder \
    .appName("CryptoStreamProcessor") \
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

def write_to_mongo(df, epoch_id):
    df.write \
      .format("mongo") \
      .mode("append") \
      .option("uri", "mongodb://mongodb:27017/crypto_db.raw_transactions") \
      .save()

query = value_df.writeStream \
    .foreachBatch(write_to_mongo) \
    .outputMode("append") \
    .start()

query.awaitTermination()