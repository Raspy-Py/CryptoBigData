# TODO
1. Remove postgress
2. Make batch processing job start automatically
3. Implement all required batch precomputations
4. Implement all endpoints in fastapi-app

# Docs

## Configuration

Configure currencies you want to track in [`websocket-client/config.json`](./websocket-client/config.json) file:
```json
{
    "currencies": ["XBTUSD", "ETHUSD", "LTCUSD"]
}
```

## Deployment

`1.1`
**For the first start: create network**
```bash
docker network create crypto-net
```

`1.2`
**For succesive launches: start the network**
```bash
docker network up crypto-net
```
`2`
**Launch the application**
```bash
docker-compose up --build
```
`3`
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
