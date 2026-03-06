import websocket
import json
from kafka import KafkaProducer
import sys

FINNHUB_API_KEY = "ur_finnhub_api"
KAFKA_BROKER = "ec2_public_ip:9093"
TOPIC_NAME = "stock_market_topic"

SYMBOLS = ["AAPL", "AMZN", "TSLA"] 

try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(3, 8, 0)
    )
    print(f"Connected to Kafka at {KAFKA_BROKER}")
except Exception as e:
    print(f"Kafka connection error: {e}")
    sys.exit(1)

message_count = 0

def on_message(ws, message):
    global message_count
    try:
        data = json.loads(message)
        
        if data.get('type') == 'trade':
            for trade in data['data']:
                payload = {
                    "symbol": trade['s'],
                    "price": trade['p'],
                    "volume": trade['v'],
                    "timestamp": trade['t'],
                    "source": "finnhub_live"
                }
                
                producer.send(TOPIC_NAME, value=payload)
                message_count += 1
                
                print(f"Kafka [{message_count}] → {payload['symbol']}: ${payload['price']:.2f} | Vol: {payload['volume']}")
                
    except Exception as e:
        print(f"Message processing error: {e}")

def on_open(ws):
    print("Connected to Finnhub LIVE WebSocket")
    print(f"Monitoring: {', '.join(SYMBOLS)}")
    
    for sym in SYMBOLS:
        ws.send(json.dumps({"type": "subscribe", "symbol": sym}))

def on_error(ws, error):
    print(f"WebSocket error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("Finnhub disconnected")

def run_producer():
    websocket.enableTrace(False)
    ws_url = f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}"
    
    ws = websocket.WebSocketApp(
        ws_url,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_open=on_open
    )
    
    try:
        ws.run_forever(ping_interval=20, ping_timeout=10)
    except KeyboardInterrupt:
        print("\nManual stop (Ctrl+C)...")
        producer.flush()
        producer.close()

if __name__ == "__main__":
    run_producer()
