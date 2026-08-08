"""
src/fraud_alerts_consumer.py

Consumes banking.fraud.alerts (populated by the silver cleaning Spark job's
fraud_stream) and writes each event into audit.fraud_alerts in Postgres,
so the dashboard can query it with plain SQL instead of needing a live
Kafka/Spark connection.

Run this as a standalone long-running process (e.g. in its own terminal,
or as a small Airflow task/container) - it polls Kafka continuously.

Usage:
    python src/fraud_alerts_consumer.py --pg-conn "postgresql://etl_user:etl_password@localhost:5433/etl_db"
"""
from __future__ import annotations

import argparse
import json

import psycopg2
from kafka import KafkaConsumer


def main() -> None:
    parser = argparse.ArgumentParser(description="Consume banking.fraud.alerts into Postgres")
    parser.add_argument("--pg-conn", required=True)
    parser.add_argument("--kafka-bootstrap", default="localhost:9092",
                         help="Use localhost:9092 if running this script on your host machine "
                              "(matches KAFKA_ADVERTISED_LISTENERS PLAINTEXT_HOST in docker-compose.yml)")
    parser.add_argument("--topic", default="banking.fraud.alerts")
    args = parser.parse_args()

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.kafka_bootstrap,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=10000,  # stop after 10s of no new messages, for one-shot batch runs
    )

    conn = psycopg2.connect(args.pg_conn)
    cur = conn.cursor()

    count = 0
    for message in consumer:
        event = message.value
        cur.execute("""
            INSERT INTO audit.fraud_alerts
            (transaction_id, account_id, customer_id, channel,
             merchant_category_code, amount, status, event_ts)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            event.get("transaction_id"),
            event.get("account_id"),
            event.get("customer_id"),
            event.get("channel"),
            event.get("merchant_category_code"),
            event.get("amount"),
            event.get("status"),
            event.get("transaction_ts") or event.get("event_ts"),
        ))
        count += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"Inserted {count} fraud alert(s) into audit.fraud_alerts")


if __name__ == "__main__":
    main()
    