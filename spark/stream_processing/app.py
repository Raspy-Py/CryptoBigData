from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, LongType

# Initialize Spark session with Cassandra connection
spark = SparkSession.builder \
    .appName("CryptoStreamProcessor") \
    .config("spark.cassandra.connection.host", "cassandradb") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("side", StringType(), True),
    StructField("size", IntegerType(), True),
    StructField("price", FloatType(), True),
    #StructField("tickDirection", StringType(), True),
    # StructField("grossValue", LongType(), True),
    # StructField("homeNotional", FloatType(), True),
    # StructField("foreignNotional", FloatType(), True),
    # StructField("trdType", StringType(), True)
])

kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "crypto_data") \
    .load()

value_df = kafka_df.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), schema).alias("data")) \
    .select("data.*")

value_df.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="raw_transactions", keyspace="crypto_space") \
    .option("checkpointLocation", "/path/to/checkpoint/dir") \
    .start() \
    .awaitTermination()
