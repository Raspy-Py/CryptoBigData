# TODO
1. Make batch processing job start automatically
1. Implement all required batch precomputations
1. Implement all endpoints in fastapi-app

# Docs

## Design
Out system consists of several conteinerised components that we run using single docker-compose.
It consists of: 1) As a message queue we picked Kafka because we previously worked with it and it was easy to setup and debug. Specifically we create a topic called crypto_data and a Producer that writes data from web socket to it 2) Websocket microservice that writes the contunious stream of data to our Kafka topic 3) As a database we used Mongo 4) Spark that is used for stream and batch processing of data. 5) We create separate container for Spark stream processing of messages from kafka topic. It essentially formats messages and writes it to our database into the raw_transactions table 6) We have separate container for batch processing, it aggregates data according to given requirments and writes it to aggregated_transactions
7) We also run a microservice for REST API that access data in our database and returns it via GET requests

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
