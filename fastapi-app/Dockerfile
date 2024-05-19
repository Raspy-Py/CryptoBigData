FROM python:3.9-slim

WORKDIR /app

COPY main.py .
COPY wait-for-postgres.sh .

RUN apt-get update && apt-get install -y \
    libpq-dev \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

RUN pip install fastapi uvicorn pymongo psycopg2-binary

CMD ["./wait-for-postgres.sh", "postgres", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
