#!/bin/bash

echo "Waiting for Kafka to be ready..."
sleep 10

spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 \
  --executor-memory 2G \
  --total-executor-cores 1 \
  /opt/app/app.py

exec "$@"