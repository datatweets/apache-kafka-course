#!/usr/bin/env python3
"""
Module 9 — Script 02: MySQL to Kafka Producer
==============================================
Reads rows from two MySQL tables and publishes each row as a JSON event
to a dedicated Kafka topic.

Concepts covered:
  - Polling a relational database for new rows using a high-water mark (last seen id)
  - Wrapping each MySQL row in an envelope (source metadata + payload)
  - Choosing the Kafka message key (MySQL primary key) for partition routing
  - Serialising a Python dict to JSON bytes
  - Running continuously with a configurable poll interval
  - One-shot mode for testing

Pipeline position:
  MySQL  ──►  THIS SCRIPT  ──►  Kafka  ──►  Elasticsearch

Topics produced to:
  m9-mysql-customers   (customers table rows)
  m9-mysql-orders      (orders table rows)

Run (one-shot, reads all current rows and exits):
  python 02_mysql_to_kafka_producer.py --once

Run (continuous polling every 5 seconds):
  python 02_mysql_to_kafka_producer.py

Prerequisites:
  - docker compose --profile pipeline up -d --build (from docker/)
  - Topics created by create-topics.sh
  - .venv activated
"""

import argparse
import json
import time
from datetime import datetime, timezone
from decimal import Decimal

import mysql.connector
from kafka import KafkaProducer

# ---------------------------------------------------------------------------
# MySQL connection — host:3307 maps to the container's port 3306.
# ---------------------------------------------------------------------------
MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3307,
    "user": "kafka",
    "password": "kafka123",
    "database": "kafka_course",
}

# ---------------------------------------------------------------------------
# Kafka bootstrap — localhost:9092/9093/9094 are the PLAINTEXT_HOST listeners,
# i.e. the external-facing ports on the Docker host.
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092", "localhost:9093", "localhost:9094"]

# ---------------------------------------------------------------------------
# Table → topic mapping.
# Each table has its own Kafka topic so consumers can subscribe independently.
# ---------------------------------------------------------------------------
TABLE_TO_TOPIC = {
    "customers": "m9-mysql-customers",
    "orders": "m9-mysql-orders",
}


# ---------------------------------------------------------------------------
# Custom JSON serialiser.
#
# json.dumps() cannot handle Decimal (MySQL DECIMAL type) or datetime objects
# out of the box.  We convert:
#   Decimal  →  float   (acceptable precision for amount/price columns)
#   datetime →  ISO-8601 string (compatible with Elasticsearch date fields)
# ---------------------------------------------------------------------------
def json_default(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"Cannot serialise type {type(value).__name__}")


def create_producer() -> KafkaProducer:
    """
    Build a KafkaProducer configured for reliable delivery.

    key_serializer:
      The key is the MySQL row id (an integer).  We convert it to a string
      first, then encode as UTF-8 bytes.  This keeps the key human-readable
      in Kafdrop and consistent with Elasticsearch document ids.

    value_serializer:
      The value is a Python dict.  json.dumps converts it to a JSON string,
      then encode() converts the string to bytes.

    acks="all":
      Wait for all in-sync replicas to confirm the write.  This ensures
      no data is lost even if the leader broker crashes immediately after
      acknowledging.

    retries=5:
      Retry transient failures (e.g. leader election) up to 5 times.
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=lambda key: str(key).encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, default=json_default).encode("utf-8"),
        acks="all",
        retries=5,
    )


def fetch_new_rows(table: str, last_seen_id: int) -> list[dict]:
    """
    Return all rows from `table` where id > last_seen_id, ordered by id.

    High-water mark polling:
      We track the highest id we have seen per table.  On each poll cycle
      we only fetch rows with a higher id.  This avoids re-publishing rows
      that were already sent.

    Limitation:
      This approach only captures INSERT operations (new rows).  It does NOT
      capture UPDATE or DELETE.  For full change capture, use Debezium or
      Kafka Connect with a CDC connector (see the Kafka Connect lab).
    """
    query = f"SELECT * FROM {table} WHERE id > %s ORDER BY id ASC"

    # Open a fresh connection per query.  In a high-frequency production
    # scenario, use a connection pool instead.
    connection = mysql.connector.connect(**MYSQL_CONFIG)
    try:
        # dictionary=True returns rows as dicts (column name → value)
        # rather than plain tuples.  This makes it trivial to serialise
        # directly to JSON without manually naming columns.
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query, (last_seen_id,))
        return cursor.fetchall()
    finally:
        connection.close()


def build_envelope(table: str, row: dict) -> dict:
    """
    Wrap a MySQL row in a metadata envelope before publishing to Kafka.

    The envelope pattern adds context that consumers need:
      - source: which system the data came from
      - table: which table the row belongs to
      - operation: what happened (here always snapshot_or_insert)
      - emitted_at: when this event was produced (producer-side timestamp)
      - data: the actual MySQL row

    This pattern is industry-standard in CDC pipelines.  Debezium uses a
    similar envelope structure.  Adding it here teaches learners what the
    envelope is before they see Debezium produce it automatically.
    """
    return {
        "source": "mysql",
        "table": table,
        "operation": "snapshot_or_insert",
        "emitted_at": datetime.now(timezone.utc).isoformat(),
        "data": row,
    }


def publish_table(producer: KafkaProducer, table: str, last_seen_id: int) -> int:
    """
    Fetch and publish all new rows for one table.
    Returns the updated high-water mark (highest id published).
    """
    rows = fetch_new_rows(table, last_seen_id)
    topic = TABLE_TO_TOPIC[table]
    latest_id = last_seen_id

    for row in rows:
        key = row["id"]
        envelope = build_envelope(table, row)

        # send() is non-blocking.  We do not call .get() here so that
        # multiple rows are batched together for efficiency.
        producer.send(topic, key=key, value=envelope)

        print(f"  [{table}] id={key}  →  topic={topic}")
        latest_id = max(latest_id, key)

    if rows:
        # flush() blocks until all buffered records for this batch are
        # acknowledged by the broker before we move on to the next table.
        producer.flush()
        print(f"  Flushed {len(rows)} record(s) from '{table}' to '{topic}'")

    return latest_id


def main():
    parser = argparse.ArgumentParser(description="Publish MySQL rows to Kafka.")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Publish current rows and exit (no continuous polling).",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=5,
        help="Poll interval in seconds when running continuously (default: 5).",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("  Module 9 — MySQL → Kafka Producer")
    print(f"  Mode: {'one-shot' if args.once else f'continuous (every {args.interval}s)'}")
    print("=" * 60)

    producer = create_producer()

    # High-water marks: we start from 0 so the first poll reads everything.
    last_seen: dict[str, int] = {table: 0 for table in TABLE_TO_TOPIC}

    try:
        while True:
            print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Polling MySQL ...")

            for table in TABLE_TO_TOPIC:
                last_seen[table] = publish_table(producer, table, last_seen[table])

            if args.once:
                print("\nOne-shot mode complete.")
                break

            print(f"  Sleeping {args.interval}s before next poll ...")
            time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\nInterrupted by user.")
    finally:
        producer.close()
        print("Producer closed.")

    print("\nNext step: run 03_kafka_to_elasticsearch_consumer.py")


if __name__ == "__main__":
    main()
