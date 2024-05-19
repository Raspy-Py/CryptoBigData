FROM python:3.9-slim

WORKDIR /app

COPY websocket_client.py .
COPY config.json .

RUN pip install websocket-client confluent_kafka

CMD ["python", "websocket_client.py"]
