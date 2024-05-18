import websocket
import json
from confluent_kafka import Producer

kafka_producer = Producer({'bootstrap.servers': 'kafka:9092'})

def on_message(ws, message):
    data = json.loads(message)
    kafka_producer.produce('crypto_data', value=json.dumps(data))
    kafka_producer.flush()

def on_error(ws, error):
    print(error)

def on_close(ws):
    print("### closed ###")

def on_open(ws):
    ws.send(json.dumps({"op": "subscribe", "args": ["trade:XBTUSD"]}))

if __name__ == "__main__":
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp("wss://www.bitmex.com/realtime",
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.on_open = on_open
    ws.run_forever()
