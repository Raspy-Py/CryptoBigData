# TODO
1. Remove postgres
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
