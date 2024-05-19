from fastapi import FastAPI
from pymongo import MongoClient
from datetime import datetime, timedelta

app = FastAPI()

mongo_client = MongoClient("mongodb://mongodb:27017")
mongo_db = mongo_client.crypto_db

# just for tests
@app.get("/")
def read_root():
    return {"message": "Hello, World!"}

@app.get("/reports/transactions/hourly")
def get_hourly_transactions():
    end_time = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    start_time = end_time - timedelta(hours=6)
    
    # Convert to strings in the format 'YYYY-MM-DDTHH'
    end_time_str = end_time.strftime('%Y-%m-%dT%H')
    start_time_str = start_time.strftime('%Y-%m-%dT%H')

    transactions = mongo_db.aggregated_transactions.find({
        "hour": {"$gte": start_time_str, "$lt": end_time_str}
    })
    
    result = []
    for transaction in transactions:
        result.append({
            "symbol": transaction["symbol"],
            "num_transactions": transaction["num_transactions"],
            "total_volume": transaction["total_volume"]
        })
    
    return result

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
