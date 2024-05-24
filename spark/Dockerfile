FROM bitnami/spark:latest

COPY ./${SPARK_JOB}/* /app/

ENV SPARK_VERSION=3.2.0

CMD [ "/bin/bash", "/app/submit.sh"]
