# TODO
1. Make batch processing job start automatically
1. Implement all required batch precomputations
1. Implement all endpoints in fastapi-app

# Docs

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
`2`
**Start batch processing job**
```bash
docker-compose run spark-streaming spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 /app/batch_processing.py
```

## Debug
`1`
**Build debug tools**
```bash
docker build -t admin-tools:latest ./admin-tools
```

`2`
**Launch admin console**
```bash
docker run --rm -it --network=crypto-net -v $(pwd)/admin-tools:/app admin-tools:latest
```
