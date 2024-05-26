from fastapi import FastAPI
from pymongo import MongoClient
from datetime import datetime, timedelta, timezone

app = FastAPI()

mongo_client = MongoClient("mongodb://mongodb:27017")
mongo_db = mongo_client.crypto_db

# Just for tests
@app.get("/")
def read_root():
    return {"message": "Hello, World!"}

# Helper function to format time
def format_time(dt):
    return dt.strftime('%Y-%m-%dT%H')

# 1. Number of transactions for each cryptocurrency for each hour in the last 6 hours, excluding the previous hour
@app.get("/reports/transactions/hourly")
def get_hourly_transactions():
    end_time = datetime.now(timezone.utc) + timedelta(hours=1)#.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)  # Exclude the previous hour
    start_time = end_time - timedelta(hours=6)
    print(f"Looking for transactions between {format_time(start_time)} and {format_time(end_time)}")
    
    transactions = mongo_db.aggregated_transactions.find({
        "hour": {"$gte": format_time(start_time), "$lt": format_time(end_time)}
    })
    
    result = []
    for transaction in transactions:
        result.append({
            "symbol": transaction["symbol"],
            "hour": transaction["hour"],
            "num_transactions": transaction["num_transactions"]
        })
    
    return result

# 2. Total trading volume for each cryptocurrency for the last 6 hours, excluding the previous hour
@app.get("/reports/volume/last_6_hours")
def get_volume_last_6_hours():
    end_time = datetime.now(timezone.utc) + timedelta(hours=1)#.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)  # Exclude the previous hour
    start_time = end_time - timedelta(hours=6)
    print(f"Looking for volume between {format_time(start_time)} and {format_time(end_time)}")
    volumes = mongo_db.aggregated_transactions.aggregate([
        {
            "$match": {
                "hour": {"$gte": format_time(start_time), "$lt": format_time(end_time)}
            }
        },
        {
            "$group": {
                "_id": "$symbol",
                "total_volume": {"$sum": "$total_volume"}
            }
        }
    ])
    
    result = []
    for volume in volumes:
        result.append({
            "symbol": volume["_id"],
            "total_volume": volume["total_volume"]
        })
    
    return result

# 3. Number of trades and their total volume for each hour in the last 12 hours, excluding the current hour
@app.get("/reports/trades/last_12_hours")
def get_trades_last_12_hours():
    end_time = datetime.now(timezone.utc) + timedelta(hours=1)#.replace(minute=0, second=0, microsecond=0)  # Exclude the current hour
    start_time = end_time - timedelta(hours=12)
    print(f"Looking for trades between {format_time(start_time)} and {format_time(end_time)}")
    trades = mongo_db.aggregated_transactions.find({
        "hour": {"$gte": format_time(start_time), "$lt": format_time(end_time)}
    })
    
    result = []
    for trade in trades:
        result.append({
            "symbol": trade["symbol"],
            "hour": trade["hour"],
            "num_transactions": trade["num_transactions"],
            "total_volume": trade["total_volume"]
        })
    
    return result

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
