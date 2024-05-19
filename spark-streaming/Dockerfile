FROM bitnami/spark:latest

COPY *.py /app/

ENV SPARK_VERSION=3.2.0
ENV SPARK_MONGO_PACKAGE="org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"
ENV SPARK_KAFKA_PACKAGE="org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0"

ENTRYPOINT [ "/opt/bitnami/scripts/spark/entrypoint.sh" ]
CMD [ "spark-submit", "--packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0", "--master", "local[2]", "/app/spark_streaming.py" ]
