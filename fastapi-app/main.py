from fastapi import FastAPI
from cassandra.cluster import Cluster
from datetime import datetime, timedelta

app = FastAPI()

cluster = Cluster(['cassandra'])
session = cluster.connect('crypto_space')

# just for tests
@app.get("/")
def read_root():
    return {"message": "Hello, World!"}

# --- precomputed reports ---

@app.get("/reports/transactions/hourly")
def get_hourly_transactions():
    end_time = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    start_time = end_time - timedelta(hours=6)
    
    # Convert to strings in the format 'YYYY-MM-DDTHH'
    end_time_str = end_time.strftime('%Y-%m-%dT%H')
    start_time_str = start_time.strftime('%Y-%m-%dT%H')

    query = f"""
    SELECT symbol, hour, num_transactions, total_volume
    FROM aggregated_transactions
    WHERE hour >= '{start_time_str}' AND hour < '{end_time_str}'
    ALLOW FILTERING
    """
    rows = session.execute(query)

    result = []
    for row in rows:
        result.append({
            "symbol": row.symbol,
            "hour": row.hour,
            "num_transactions": row.num_transactions,
            "total_volume": row.total_volume
        })
    
    return result

# --- ad-hoc queries ---
#@app.get('/ad-hoc/transactions')

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
