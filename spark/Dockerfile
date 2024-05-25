FROM bitnami/spark:3

USER root

COPY stream_processing/submit.sh /opt/bitnami/scripts/spark/entrypoint.sh

COPY stream_processing/app.py /opt/app/spark_streaming.py

RUN chmod +x /opt/bitnami/scripts/spark/entrypoint.sh

ENTRYPOINT ["/opt/bitnami/scripts/spark/entrypoint.sh"]
