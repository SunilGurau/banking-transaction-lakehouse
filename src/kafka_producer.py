import os
import json
import time
import glob
from confluent_kafka import Producer

# Configuration
KAFKA_BROKER = 'localhost:9092' # Use 'kafka:29092' if running inside Docker
TOPIC_NAME = 'banking.transactions.raw'
STREAMING_DATA_DIR = './data/streaming/'

def delivery_report(err, msg):
    if err is not None:
        print(f" Delivery failed: {err}")
    else:
        print(f" Sent to {msg.topic()} -> {msg.value().decode('utf-8')[:60]}...")

def stream_transactions():
    producer = Producer({'bootstrap.servers': KAFKA_BROKER})
    
    files = glob.glob(os.path.join(STREAMING_DATA_DIR, '*.jsonl'))
    if not files:
        print(" No JSONL files found.")
        return

    print(f" Streaming {len(files)} files to Kafka...")
    
    try:
        for file_path in files:
            with open(file_path, 'r') as file:
                for line in file:
                    record = json.loads(line.strip())
                    
                    producer.produce(
                        topic=TOPIC_NAME,
                        value=json.dumps(record).encode('utf-8'),
                        callback=delivery_report
                    )
                    producer.poll(0)
                    time.sleep(0.3) # Simulates real-time flow
                    
        producer.flush()
        print(" Streaming complete.")
        
    except KeyboardInterrupt:
        print("\n Stopped manually.")
        producer.flush()

if __name__ == '__main__':
    stream_transactions()