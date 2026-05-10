#!/usr/bin/env python3
"""
Module 9 — Script 04: Pipeline Monitor & Live Demo
===================================================
A single interactive script that:
  1. Inserts a new row into MySQL (customers or orders)
  2. Prints a countdown showing the pipeline lag until the row appears
     in both Kafka and Elasticsearch

Use this script to experience the end-to-end pipeline latency live:
  MySQL INSERT  ──►  mysql_to_kafka_producer  ──►  Kafka  ──►  kafka_to_es_consumer  ──►  Elasticsearch

It also shows how to query Elasticsearch via the HTTP API and how to
read the latest offset from a Kafka topic using the KafkaAdminClient.

Run:
  # Add a customer and watch it propagate:
  python 04_pipeline_monitor.py --table customers --name "Fatima Al-Amin" --country Morocco

  # Add an order and watch it propagate:
  python 04_pipeline_monitor.py --table orders --product "Wireless Keyboard" --amount 79.99 --customer-id 1

  # Just watch existing Kafka topic offsets and ES doc counts:
  python 04_pipeline_monitor.py --watch-only

Prerequisites:
  - docker compose --profile pipeline up -d --build (from docker/)
  - 02_mysql_to_kafka_producer.py running in a separate terminal (--interval 5)
  - 03_kafka_to_elasticsearch_consumer.py running in a separate terminal
  - .venv activated
"""

import argparse
import time
from datetime import datetime

import mysql.connector
import requests
from kafka.admin import KafkaAdminClient
from kafka.errors import KafkaError

# ---------------------------------------------------------------------------
# Connection parameters — same as Scripts 02 and 03.
# ---------------------------------------------------------------------------
MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3307,
    "user": "kafka",
    "password": "kafka123",
    "database": "kafka_course",
}

KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092", "localhost:9093", "localhost:9094"]
ELASTICSEARCH_URL = "http://localhost:9200"

KAFKA_TOPICS = ["m9-mysql-customers", "m9-mysql-orders"]
ES_INDEXES = ["m9-customers", "m9-orders"]


# ---------------------------------------------------------------------------
# Section 1: Insert helpers
# ---------------------------------------------------------------------------

def insert_customer(name: str, country: str) -> int:
    """
    Insert a new row into the customers table and return its new id.

    We use LAST_INSERT_ID() to retrieve the auto-incremented primary key
    that MySQL assigned.
    """
    connection = mysql.connector.connect(**MYSQL_CONFIG)
    try:
        cursor = connection.cursor()
        cursor.execute(
            "INSERT INTO customers (name, email, country) VALUES (%s, %s, %s)",
            (name, f"{name.lower().replace(' ', '.')}@example.com", country),
        )
        connection.commit()
        return cursor.lastrowid
    finally:
        connection.close()


def insert_order(customer_id: int, product: str, amount: float) -> int:
    """Insert a new row into the orders table and return its new id."""
    connection = mysql.connector.connect(**MYSQL_CONFIG)
    try:
        cursor = connection.cursor()
        cursor.execute(
            "INSERT INTO orders (customer_id, product, amount, status) VALUES (%s, %s, %s, %s)",
            (customer_id, product, amount, "pending"),
        )
        connection.commit()
        return cursor.lastrowid
    finally:
        connection.close()


# ---------------------------------------------------------------------------
# Section 2: Observe Kafka topic offsets
# ---------------------------------------------------------------------------

def get_topic_end_offsets(topic: str) -> dict:
    """
    Return the end offset for each partition of a topic.

    The end offset = the offset of the next message to be written.
    end_offset - 1 = offset of the most recently written message.

    We use KafkaAdminClient.list_consumer_group_offsets to avoid consuming
    any messages — we just inspect broker-side metadata.
    """
    from kafka import KafkaConsumer
    from kafka import TopicPartition

    # A temporary consumer is the easiest way to fetch end offsets without
    # subscribing.  We use a unique group_id so we don't interfere with
    # the actual sink consumer (m9-es-sink).
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="m9-monitor",
    )
    try:
        # Assign the topic's partitions manually (no subscription).
        partitions = consumer.partitions_for_topic(topic) or set()
        tps = [TopicPartition(topic, p) for p in sorted(partitions)]
        consumer.assign(tps)
        end_offsets = consumer.end_offsets(tps)
        return {tp.partition: offset for tp, offset in end_offsets.items()}
    finally:
        consumer.close()


def total_messages_in_topic(topic: str) -> int:
    """Return the sum of end offsets across all partitions of a topic."""
    try:
        offsets = get_topic_end_offsets(topic)
        return sum(offsets.values())
    except KafkaError:
        return -1  # topic may not exist yet


# ---------------------------------------------------------------------------
# Section 3: Observe Elasticsearch document counts
# ---------------------------------------------------------------------------

def get_es_doc_count(index_name: str) -> int:
    """
    Return the number of documents in an Elasticsearch index.

    GET /index_name/_count returns {"count": N, "_shards": {...}}.
    A 404 (index doesn't exist yet) is handled gracefully.
    """
    try:
        response = requests.get(f"{ELASTICSEARCH_URL}/{index_name}/_count", timeout=5)
        if response.status_code == 404:
            return 0
        response.raise_for_status()
        return response.json()["count"]
    except requests.RequestException:
        return -1


def check_document_in_es(index_name: str, doc_id: str) -> bool:
    """Check if a specific document id exists in an Elasticsearch index."""
    try:
        response = requests.get(
            f"{ELASTICSEARCH_URL}/{index_name}/_doc/{doc_id}", timeout=5
        )
        return response.status_code == 200
    except requests.RequestException:
        return False


# ---------------------------------------------------------------------------
# Section 4: Watch-only mode — print a live dashboard
# ---------------------------------------------------------------------------

def print_dashboard():
    """Print a one-time snapshot of Kafka offsets and ES doc counts."""
    print("\n" + "=" * 60)
    print(f"  Pipeline Status  [{datetime.now().strftime('%H:%M:%S')}]")
    print("=" * 60)

    print("\n  Kafka topic message counts:")
    for topic in KAFKA_TOPICS:
        count = total_messages_in_topic(topic)
        label = str(count) if count >= 0 else "unavailable"
        print(f"    {topic:<30} {label} messages")

    print("\n  Elasticsearch document counts:")
    for index_name in ES_INDEXES:
        count = get_es_doc_count(index_name)
        label = str(count) if count >= 0 else "unavailable"
        print(f"    {index_name:<30} {label} documents")

    print()


def watch_loop():
    """Refresh the dashboard every 3 seconds until Ctrl+C."""
    print("Watching pipeline (Ctrl+C to stop) ...")
    try:
        while True:
            print_dashboard()
            time.sleep(3)
    except KeyboardInterrupt:
        print("\nStopped.")


# ---------------------------------------------------------------------------
# Section 5: Post-insert tracking
# ---------------------------------------------------------------------------

def track_propagation(table: str, row_id: int, timeout_seconds: int = 60):
    """
    After inserting a MySQL row, poll Kafka + ES until the row appears
    in both, then report the end-to-end latency.

    This is a simplified "integration test" showing that each stage of
    the pipeline picked up the new row.
    """
    kafka_topic = "m9-mysql-customers" if table == "customers" else "m9-mysql-orders"
    es_index = "m9-customers" if table == "customers" else "m9-orders"
    doc_id = str(row_id)

    # Snapshot the topic offset BEFORE the insert so we can verify a new
    # message was added (offset increased).
    baseline_offset = total_messages_in_topic(kafka_topic)

    print(f"\n  Inserted {table} id={row_id}")
    print(f"  Kafka baseline offset in '{kafka_topic}': {baseline_offset}")
    print(f"\n  Waiting for row to appear in Kafka and Elasticsearch ...")
    print("  (Make sure Script 02 and Script 03 are running in other terminals!)\n")

    insert_time = time.time()
    kafka_detected = False
    es_detected = False

    deadline = insert_time + timeout_seconds

    while time.time() < deadline:
        current_offset = total_messages_in_topic(kafka_topic)

        if not kafka_detected and current_offset > baseline_offset:
            kafka_lag = time.time() - insert_time
            print(f"  ✓ Kafka: offset {current_offset}  (lag: {kafka_lag:.1f}s)")
            kafka_detected = True

        if not es_detected and check_document_in_es(es_index, doc_id):
            es_lag = time.time() - insert_time
            print(f"  ✓ Elasticsearch: doc '{doc_id}' in '{es_index}'  (lag: {es_lag:.1f}s)")
            es_detected = True

        if kafka_detected and es_detected:
            print("\n  Pipeline propagation complete.")
            break

        time.sleep(1)
    else:
        if not kafka_detected:
            print("  ✗ Row did NOT appear in Kafka within timeout.")
            print("    Check that Script 02 is running.")
        if not es_detected:
            print("  ✗ Row did NOT appear in Elasticsearch within timeout.")
            print("    Check that Script 03 is running.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Module 9 Pipeline Monitor and Demo Inserter.")
    parser.add_argument("--table", choices=["customers", "orders"], help="Which table to insert into.")
    parser.add_argument("--name", help="Customer name (table=customers).")
    parser.add_argument("--country", default="Unknown", help="Customer country (table=customers).")
    parser.add_argument("--product", help="Product name (table=orders).")
    parser.add_argument("--amount", type=float, default=0.0, help="Order amount (table=orders).")
    parser.add_argument("--customer-id", type=int, default=1, help="Customer id for order (table=orders).")
    parser.add_argument(
        "--watch-only",
        action="store_true",
        help="Only display the dashboard; do not insert any data.",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("  Module 9 — Pipeline Monitor & Demo")
    print("=" * 60)

    if args.watch_only:
        watch_loop()
        return

    # Insert and track
    if args.table == "customers":
        if not args.name:
            parser.error("--name is required when --table=customers")
        row_id = insert_customer(args.name, args.country)

    elif args.table == "orders":
        if not args.product:
            parser.error("--product is required when --table=orders")
        row_id = insert_order(args.customer_id, args.product, args.amount)

    else:
        # No table specified — show dashboard and exit
        print_dashboard()
        print("Tip: use --table customers/orders to insert data, or --watch-only for continuous monitoring.")
        return

    track_propagation(args.table, row_id)


if __name__ == "__main__":
    main()
