from kafka import KafkaConsumer
import boto3
import json
from datetime import datetime
import sys

KAFKA_BROKER = "ec2_public_ip:9093"
S3_BUCKET = "unic_name_for_ur_s3_bucket"
TOPIC_NAME = "stock_market_topic"
BATCH_SIZE = 50

try:
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        api_version=(3, 8, 0),
        auto_offset_reset='latest'
    )
    print(f"Connected to Kafka! Listening to topic: {TOPIC_NAME}")
except Exception as e:
    print(f"Kafka Consumer connection error: {e}")
    sys.exit(1)

try:
    s3 = boto3.client('s3')
    print(f"Connected to AWS S3 (Bucket: {S3_BUCKET})")
except Exception as e:
    print(f"S3 configuration error (check AWS credentials via 'aws configure'): {e}")
    sys.exit(1)

def upload_to_s3(batch, count):
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"stock_live_{ts}_{count}.json"
    
    jsonl_data = ""
    for record in batch:
        jsonl_data += json.dumps(record) + "\n"
    
    try:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=f"kafka-stock-data/{filename}",
            Body=jsonl_data,
            ContentType='application/json'
        )
        print(f"SAVED: {len(batch)} records were uploaded to S3 as {filename}")
    except Exception as e:
        print(f"Error saving to S3: {e}")

if __name__ == "__main__":
    print(f"\nConsumer is running. Waiting for data from Finnhub -> Kafka...\n")
    batch = []
    total_batches = 0
    
    try:
        for msg in consumer:
            batch.append(msg.value)
            print(f"Received from Kafka: {msg.value.get('symbol', 'UNKNOWN')} - ${msg.value.get('price', 0)}")
            
            if len(batch) >= BATCH_SIZE:
                total_batches += 1
                upload_to_s3(batch, total_batches)
                batch = []
                
    except KeyboardInterrupt:
        if batch: 
            total_batches += 1
            print("\nSaving final batch before stopping...")
            upload_to_s3(batch, total_batches)
        print("\nConsumer stopped.")
