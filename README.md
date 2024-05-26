# `PLS, GIVE US 13.72 points... 🙏🥹🥲🙏`
We've spent an enourmous amount of human-hours trying to fire up Cassandra to work with Spark and failed, coming back to MongoDB..

# Docs

## Design

Our system consists of several containerized components, all orchestrated using a single Docker Compose setup. The components are as follows:

1. **Kafka**: We chose Kafka as our message queue due to our familiarity with it and its ease of setup and debugging. We created a topic called `crypto_data` and a Producer that writes data from a WebSocket to this topic.
2. **WebSocket Microservice**: This microservice continuously streams data to our Kafka topic `crypto_data`.
3. **MongoDB**: We use MongoDB as our database to store the processed data. We choose mongo beacause data stream that we receive from the websocekt might have different structure and document based DBs behave well in such cases.
4. **Spark**: Spark is utilized for both stream and batch processing of data.
5. **Spark Stream Processing**: This container processes messages from the Kafka topic. It formats the messages and writes them to the `raw_transactions` collection in MongoDB.
6. **Spark Batch Processing**: This container aggregates data according to specified requirements and writes the results to the `aggregated_transactions` collection in MongoDB.
7. **REST API Microservice**: This microservice provides REST API endpoints to access the data in our database and return it via GET requests. It uses Pythond and FastAPI framework. We choose it because we know python and FastAPI is a very comprehensive and handy framework.

Each component is designed to work together seamlessly, ensuring efficient data processing and retrieval.

The table structure can be observed from diagram

## Diagram of the architecture
![alt text](img/bigDataProjectArchitecture.drawio.png)

## Endpoints

### Reports
- **GET /reports/transactions/hourly**
  - Description: Get the number of transactions for each cryptocurrency for each hour in the last 6 hours, excluding the previous hour.
  - Response: List of transactions with symbol, hour, and number of transactions.

- **GET /reports/volume/last_6_hours**
  - Description: Get the total trading volume for each cryptocurrency for the last 6 hours, excluding the previous hour.
  - Response: List of volumes with symbol and total volume.

- **GET /reports/trades/last_12_hours**
  - Description: Get the number of trades and their total volume for each hour in the last 12 hours, excluding the current hour.
  - Response: List of trades with symbol, hour, number of transactions, and total volume.

### Ad-Hoc Queries
- **GET /ad-hoc/transactions/{symbol}/{minutes}**
  - Description: Get the number of trades processed in a specific cryptocurrency in the last N minutes, excluding the last minute.
  - Parameters:
    - `symbol`: Cryptocurrency symbol.
    - `minutes`: Number of minutes.
  - Response: `{"trade_count": <number>}`

- **GET /ad-hoc/top-cryptos/{n}**
  - Description: Get the top N cryptocurrencies with the highest trading volume in the last hour.
  - Parameters:
    - `n`: Number of top cryptocurrencies.
  - Response: `{"top_cryptos": <list>}`

- **GET /ad-hoc/current-price/{symbol}**
  - Description: Get the current buy and sell price of a specific cryptocurrency.
  - Parameters:
    - `symbol`: Cryptocurrency symbol.
  - Response: `{"symbol": <symbol>, "current_buy_price": <price>, "current_sell_price": <price>}`


## Results

### Category A endpoints:
`1` **Return the aggregated statistics containing the number of transactions for each cryptocurrency for each hour in the last 6 hours, excluding the previous hour.**<br>
<img src="img/category-A-1.png" alt="alt text" width="80%">

`2` **Return the statistics about the total trading volume for each cryptocurrency for the last 6 hours, excluding the previous hour.**<br>
<img src="img/category-A-2.png" alt="alt text" width="80%">

`3` **Return aggregated statistics containing the number of trades and their total volume for each hour in the last 12 hours, excluding the current hour.**<br>
<img src="img/category-A-3.png" alt="alt text" width="80%">

### Category B endpoints:
`1` **Return the number of trades processed in a specific cryptocurrency in the last N minutes, excluding the last minute.**<br>
<img src="img/numberOfTradesPerSymbolInMin.png" alt="alt text" width="80%">

`2` **Return the top N cryptocurrencies with the highest trading volume in the last hour.**<br>
<img src="img/topCrypto.png" alt="alt text" width="80%">

`3` **Return the cryptocurrency’s current price for «Buy» and «Sell» sides based on its symbol.**<br>
<img src="img/buySell.png" alt="alt text" width="80%">



## Configuration

Configure currencies you want to track in [`websocket-client/config.json`](./websocket-client/config.json) file:
```json
{
    "currencies": ["XBTUSD", "ETHUSD", "LTCUSD"]
}
```

## Deployment
`1`
**Launch the application**
```bash
docker-compose up --build -d
```


## Debug
`1`
**Launch admin console**
```bash
./admin-tools/start.sh
```
