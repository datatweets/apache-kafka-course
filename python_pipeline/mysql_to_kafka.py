#!/usr/bin/env python3
"""
Read rows from MySQL and publish them to Kafka as JSON.

This is a teaching example for the course pipeline:
MySQL -> Kafka -> Elasticsearch
"""

import argparse
import json
import time
from datetime import datetime, timezone
from decimal import Decimal

import mysql.connector
from kafka import KafkaProducer


MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3307,
    "user": "kafka",
    "password": "kafka123",
    "database": "kafka_course",
}

KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092", "localhost:9093", "localhost:9094"]

TOPICS = {
    "customers": "python.mysql.customers",
    "orders": "python.mysql.orders",
}


def json_default(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=lambda value: str(value).encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, default=json_default).encode("utf-8"),
        acks="all",
        retries=5,
    )


def fetch_rows(table, last_seen_id):
    query = f"""
        SELECT *
        FROM {table}
        WHERE id > %s
        ORDER BY id ASC
    """
    connection = mysql.connector.connect(**MYSQL_CONFIG)
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query, (last_seen_id,))
        return cursor.fetchall()
    finally:
        connection.close()


def publish_table(producer, table, last_seen_id):
    rows = fetch_rows(table, last_seen_id)
    topic = TOPICS[table]
    latest_id = last_seen_id

    for row in rows:
        event = {
            "source": "mysql",
            "table": table,
            "operation": "snapshot_or_insert",
            "emitted_at": datetime.now(timezone.utc).isoformat(),
            "data": row,
        }
        producer.send(topic, key=row["id"], value=event)
        latest_id = max(latest_id, row["id"])
        print(f"published {table} id={row['id']} to {topic}")

    if rows:
        producer.flush()

    return latest_id


def main():
    parser = argparse.ArgumentParser(description="Publish MySQL rows to Kafka topics.")
    parser.add_argument("--once", action="store_true", help="Read current rows and exit.")
    parser.add_argument("--interval", type=int, default=5, help="Polling interval in seconds.")
    args = parser.parse_args()

    producer = create_producer()
    last_seen = {"customers": 0, "orders": 0}

    try:
        while True:
            for table in ("customers", "orders"):
                last_seen[table] = publish_table(producer, table, last_seen[table])

            if args.once:
                break

            time.sleep(args.interval)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
