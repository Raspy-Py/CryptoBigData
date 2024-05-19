FROM python:3.9-slim

WORKDIR /app

COPY check_db.py .

RUN pip install pymongo

CMD ["python", "check_db.py"]
