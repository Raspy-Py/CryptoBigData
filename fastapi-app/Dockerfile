FROM python:3.9-slim

WORKDIR /app

COPY main.py .

RUN apt-get update && apt-get install -y \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install fastapi uvicorn cassandra-driver

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
