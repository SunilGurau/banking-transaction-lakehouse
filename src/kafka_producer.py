import os
import json
import time
import glob
import ast
from kafka import KafkaProducer


# Configuration
KAFKA_BROKER = 'kafka:29092'
TOPIC_NAME = 'banking.transactions.raw'
STREAMING_DATA_DIR = './data/streaming/'

def stream_transactions():
    # Initialize the pure-Python Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    files = glob.glob(os.path.join(STREAMING_DATA_DIR, '*.jsonl'))
    if not files:
        print(" No JSONL files found.")
        return

    print(f" Streaming {len(files)} files to Kafka...")
    
    try:
        for file_path in files:
            with open(file_path, 'r') as file:
                for line in file:
                    line_clean = line.strip()
                    if not line_clean:
                        continue # Skip empty lines
                    
                    # 1. Try strict JSON
                    try:
                        record = json.loads(line_clean)
                    except json.decoder.JSONDecodeError:
                        # 2. Try handling single-quoted Python dicts
                        try:
                            record = ast.literal_eval(line_clean)
                        except (SyntaxError, ValueError):
                            # 3. Complete garbage data detected! Wrap it safely for the DLQ.
                            print(" Unparseable garbage data detected! Wrapping for DLQ...")
                            record = {
                                "transaction_id": None,
                                "account_id": None,
                                "error": "malformed_payload",
                                "raw_data": line_clean
                            }
                    
                    # Send the safely packaged record to the topic
                    producer.send(TOPIC_NAME, value=record)
                    
                    print(f" Sent to {TOPIC_NAME} -> {str(record)[:60]}...")
                    time.sleep(0.3) # Simulates real-time flow
                    
        producer.flush()
        print("🎉 Streaming complete.")
        
    except KeyboardInterrupt:
        print("\n Stopped manually.")
        producer.flush()

if __name__ == '__main__':
    stream_transactions()