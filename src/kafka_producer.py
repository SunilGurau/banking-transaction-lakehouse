import os
import json
import time
import glob
import ast
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Configuration
# BUG FIX: this was hardcoded to 'kafka:29092' even though docker-compose
# sets KAFKA_BROKER=kafka:29092 as an env var for this exact container --
# the env var was never actually read (os was imported but os.environ was
# never used). Harmless right now only because the hardcoded value happened
# to match; would silently break the moment someone changes the env var.
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'kafka:29092')
TOPIC_NAME = 'banking.transactions.raw'
STREAMING_DATA_DIR = './data/streaming/'


def connect_producer(retries=10, backoff_seconds=3):
    """FIX: previously there was no retry logic at all -- if this container
    won the startup race against Kafka (which was easy to do before the
    depends_on/healthcheck fix in docker-compose.yml), KafkaProducer() would
    raise immediately and the whole container would crash-loop. Retrying
    with backoff makes this resilient even to a brief Kafka blip later on."""
    for attempt in range(1, retries + 1):
        try:
            return KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k is not None else None,
                acks='all',
                retries=5,
                linger_ms=10,
            )
        except NoBrokersAvailable:
            print(f" Kafka not reachable at {KAFKA_BROKER} (attempt {attempt}/{retries}), retrying in {backoff_seconds}s...")
            time.sleep(backoff_seconds)
    raise RuntimeError(f"Could not connect to Kafka at {KAFKA_BROKER} after {retries} attempts")


def stream_transactions():
    producer = connect_producer()

    files = glob.glob(os.path.join(STREAMING_DATA_DIR, '*.jsonl'))
    if not files:
        print(f" No JSONL files found in {STREAMING_DATA_DIR}")
        return

    print(f" Streaming {len(files)} files to Kafka at {KAFKA_BROKER}...")

    sent = 0
    try:
        for file_path in files:
            with open(file_path, 'r') as file:
                for line in file:
                    line_clean = line.strip()
                    if not line_clean:
                        continue  # Skip empty lines

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
                                "raw_data": line_clean,
                            }

                    # FIX: use transaction_id as the partition key when available
                    # so records for the same transaction land on the same
                    # partition (useful for downstream ordering/dedup).
                    key = record.get("transaction_id") if isinstance(record, dict) else None

                    producer.send(TOPIC_NAME, key=key, value=record)
                    sent += 1

                    print(f" Sent to {TOPIC_NAME} -> {str(record)[:60]}...")
                    if sent % 50 == 0:
                        producer.flush()  # periodic flush so we're not holding everything in memory
                    time.sleep(0.3)  # Simulates real-time flow

        producer.flush()
        print(f"Streaming complete. Sent {sent} messages.")

    except KeyboardInterrupt:
        print("\n Stopped manually.")
        producer.flush()
    finally:
        producer.close()


if __name__ == '__main__':
    stream_transactions()