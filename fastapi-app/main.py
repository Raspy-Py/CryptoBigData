from fastapi import FastAPI
from pymongo import MongoClient
import psycopg2

app = FastAPI()

# MongoDB connection
mongo_client = MongoClient("mongodb://mongodb:27017")
mongo_db = mongo_client.crypto_db

# PostgreSQL connection
pg_conn = psycopg2.connect(database="crypto_db", user="user", password="password", host="postgres", port="5432")
pg_cursor = pg_conn.cursor()

@app.get("/")
def read_root():
    return {"message": "Hello, World!"}

@app.get("/reports/transactions/hourly")
def get_hourly_transactions():
    # Placeholder for actual implementation
    return {"data": "Hourly Transactions"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
