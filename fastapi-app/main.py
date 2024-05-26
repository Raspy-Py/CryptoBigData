from fastapi import FastAPI, HTTPException
from pymongo import MongoClient, DESCENDING
from datetime import datetime, timedelta, timezone

app = FastAPI()

MONGO_URI = "mongodb://mongodb:27017"
DATABASE_NAME = "crypto_db"
RAW_COLLECTION_NAME = "raw_transactions"

mongo_client = MongoClient(MONGO_URI)
mongo_db = mongo_client[DATABASE_NAME]
raw_collection = mongo_db[RAW_COLLECTION_NAME]

# Just for tests
@app.get("/")
def read_root():
    return {"message": "Hello, World!"}

def format_time(dt):
    return dt.strftime('%Y-%m-%dT%H')

# 1. Number of transactions for each cryptocurrency for each hour in the last 6 hours, excluding the previous hour
@app.get("/reports/transactions/hourly")
def get_hourly_transactions():
    end_time = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)  # Exclude the previous hour
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
    end_time = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)  # Exclude the previous hour
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
    end_time = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)  # Exclude the current hour
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


# -- ad-hoc queries --
# Return the number of trades processed in a specific cryptocurrency in the last N minutes, excluding the last minute. 
@app.get("/ad-hoc/transactions/{symbol}/{minutes}")
def get_trades_for_n_minutes(symbol: str, minutes: int):
    try:
        current_time = datetime.now(timezone.utc)
        start_time = current_time - timedelta(minutes=minutes + 1)
        end_time = current_time - timedelta(minutes=1)

        query = {
            "symbol": symbol,
            "timestamp": {
                "$gte": start_time.isoformat() + 'Z',
                "$lt": end_time.isoformat() + 'Z'
            }
        }

        trade_count = raw_collection.count_documents(query)
        return {"trade_count": trade_count}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
# Return the top N cryptocurrencies with the highest trading volume in the last hour.
@app.get("/ad-hoc/top-cryptos/{n}")
def get_top_n_cryptos(n: int):
    try:
        current_time = datetime.now(timezone.utc)
        start_time = current_time - timedelta(hours=1)

        pipeline = [
            {"$match": {"timestamp": {"$gte": start_time.isoformat() + 'Z'}}},
            {"$group": {"_id": "$symbol", "total_volume": {"$sum": "$size"}}},
            {"$sort": {"total_volume": -1}},
            {"$limit": n}
        ]

        top_cryptos = list(raw_collection.aggregate(pipeline))
        return {"top_cryptos": top_cryptos}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/ad-hoc/current-price/{symbol}")
def get_current_price(symbol: str):
    try:
        buy_trade = raw_collection.find_one({"symbol": symbol, "side": "Buy"}, sort=[("timestamp", DESCENDING)])
        sell_trade = raw_collection.find_one({"symbol": symbol, "side": "Sell"}, sort=[("timestamp", DESCENDING)])

        if not buy_trade or not sell_trade:
            raise HTTPException(status_code=404, detail="Prices not found for the specified symbol")

        return {
            "symbol": symbol,
            "current_buy_price": buy_trade["price"] if buy_trade else None,
            "current_sell_price": sell_trade["price"] if sell_trade else None
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
