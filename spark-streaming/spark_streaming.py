from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

spark = SparkSession.builder \
    .appName("CryptoStreamProcessor") \
    .getOrCreate()

schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", FloatType(), True),
    StructField("size", IntegerType(), True),
    StructField("timestamp", StringType(), True)
])

kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "crypto_data") \
    .load()

value_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data"))
processed_df = value_df.select("data.*")

query = processed_df.writeStream \
    .format("console") \
    .start()

query.awaitTermination()
