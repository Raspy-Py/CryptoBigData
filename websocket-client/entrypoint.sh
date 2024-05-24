#!/bin/bash

echo "Waiting for Kafka to be ready..."
sleep 20

/opt/bitnami/kafka/bin/kafka-topics.sh --create --bootstrap-server kafka:9092 --replication-factor 1 --partitions 3 --topic crypto_data
/opt/bitnami/kafka/bin/kafka-topics.sh --create --bootstrap-server kafka:9092 --replication-factor 1 --partitions 3 --topic processed

/opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 \
  /opt/app/spark_streaming.py

docker-compose run spark-streaming spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 /app/batch_processing.py

exec "$@"
