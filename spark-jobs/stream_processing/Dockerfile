FROM bitnami/spark:3

USER root

COPY entrypoint.sh /opt/bitnami/scripts/spark/entrypoint.sh

COPY app.py /opt/app/app.py

RUN chmod +x /opt/bitnami/scripts/spark/entrypoint.sh

ENTRYPOINT ["/opt/bitnami/scripts/spark/entrypoint.sh"]
