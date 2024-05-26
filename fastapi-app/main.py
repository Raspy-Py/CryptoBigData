from fastapi import FastAPI, HTTPException
from pymongo import MongoClient, DESCENDING
from datetime import datetime, timedelta

app = FastAPI()

MONGO_URI = "mongodb://mongodb:27017"
DATABASE_NAME = "crypto_db"
RAW_COLLECTION_NAME = "raw_transactions"

mongo_client = MongoClient(MONGO_URI)
mongo_db = mongo_client[DATABASE_NAME]
raw_collection = mongo_db[RAW_COLLECTION_NAME]

# just for tests
@app.get("/")
def read_root():
    return {"message": "Hello, World!"}

@app.get("/reports/transactions/hourly")
def get_hourly_transactions():
    end_time = datetime.utcnow().replace(minute=0, second=0, microsecond=0) # Round down to the hour to exclude partial hours
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


# -- ad-hoc queries --
# Return the number of trades processed in a specific cryptocurrency in the last N minutes, excluding the last minute. 
@app.get("/ad-hoc/transactions/{symbol}/{minutes}")
def get_trades_for_n_minutes(symbol: str, minutes: int):
    try:
        current_time = datetime.utcnow()
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
        current_time = datetime.utcnow()
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
