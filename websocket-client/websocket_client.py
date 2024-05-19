import websocket
import json
import os
from confluent_kafka import Producer

kafka_producer = Producer({'bootstrap.servers': 'kafka:9092'})

def on_message(ws, message):
    data = json.loads(message)
    kafka_producer.produce('crypto_data', value=json.dumps(data))
    kafka_producer.flush()

def on_error(ws, error):
    print(error)

def on_close(ws, close_status_code, close_msg):
    print("### closed ###", close_status_code, close_msg)

def on_open(ws):
    with open('config.json') as config_file:
        config = json.load(config_file)
        currencies = config.get('currencies', [])
        for currency in currencies:
            ws.send(json.dumps({"op": "subscribe", "args": [f"trade:{currency}"]}))

if __name__ == "__main__":
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp("wss://www.bitmex.com/realtime",
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.on_open = on_open
    ws.run_forever()
