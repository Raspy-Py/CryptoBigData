#!/bin/bash

spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 /app/app.py
