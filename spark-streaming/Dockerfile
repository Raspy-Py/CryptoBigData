# Dockerfile
FROM bitnami/spark:latest

COPY spark_streaming.py /app/

# Provide the Maven coordinates directly in the CMD
ENTRYPOINT [ "/opt/bitnami/scripts/spark/entrypoint.sh" ]
CMD [ "spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0", "--master", "local[2]", "/app/spark_streaming.py" ]
