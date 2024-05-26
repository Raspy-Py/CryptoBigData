import websocket
import json
import time
import os
from confluent_kafka import Producer, KafkaError

kafka_config = {
    'bootstrap.servers': 'kafka:9092'
}

kafka_producer = None
max_retries = 10
retry_interval = 5  # in seconds



def on_message(ws, message):
    data = json.loads(message)
    if 'data' in data:
        data_data = data['data']
        for data_item in data_data:
            kafka_producer.produce('crypto_data', value=json.dumps(data_item))
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
    for attempt in range(max_retries):
        try:
            kafka_producer = Producer(kafka_config)
            break
        except KafkaError as e:
            print(f"Attempt {attempt + 1} of {max_retries}: Kafka not available, retrying in {retry_interval} seconds...")
            time.sleep(retry_interval)
    else:
        print("Failed to connect to Kafka after multiple attempts, exiting.")
        exit(1)

    websocket.enableTrace(True)
    ws = websocket.WebSocketApp("wss://www.bitmex.com/realtime",
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.on_open = on_open
    ws.run_forever()
