#!/bin/bash

echo "Waiting for Cassandra to be ready..."
sleep 10

spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 \
  --executor-memory 2G \
  --total-executor-cores 1 \
  /opt/app/app.py

exec "$@"
