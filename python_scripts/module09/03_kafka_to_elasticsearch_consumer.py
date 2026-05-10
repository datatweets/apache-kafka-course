#!/usr/bin/env python3
"""
Module 9 — Script 03: Kafka to Elasticsearch Consumer
======================================================
Subscribes to two Kafka topics, reads JSON events, and indexes each event
into a matching Elasticsearch index as a document.

Concepts covered:
  - Constructing a KafkaConsumer with JSON deserialisation
  - Consumer groups — what they are and how offset tracking works
  - auto_offset_reset="earliest" — read from the beginning on first run
  - enable_auto_commit=True — Kafka commits the offset after each poll
  - Subscribing to multiple topics at once
  - Calling the Elasticsearch HTTP API to create indexes and index documents
  - Idempotent indexing: using the MySQL row id as the Elasticsearch doc id
    so reprocessing the same event updates rather than duplicates documents

Pipeline position:
  MySQL  ──►  Kafka  ──►  THIS SCRIPT  ──►  Elasticsearch

Topics consumed:
  m9-mysql-customers  →  Elasticsearch index: m9-customers
  m9-mysql-orders     →  Elasticsearch index: m9-orders

Run:
  python 03_kafka_to_elasticsearch_consumer.py

  # Stop after N messages (useful for testing):
  python 03_kafka_to_elasticsearch_consumer.py --max-messages 10

Prerequisites:
  - docker compose --profile pipeline up -d --build (from docker/)
  - 02_mysql_to_kafka_producer.py has run at least once
  - .venv activated
"""

import argparse
import json

import requests
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# ---------------------------------------------------------------------------
# Kafka bootstrap — host-facing listeners (PLAINTEXT_HOST).
# This script runs on the host machine, so we use localhost addresses.
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092", "localhost:9093", "localhost:9094"]

# ---------------------------------------------------------------------------
# Elasticsearch is also mapped to the host on port 9200.
# ---------------------------------------------------------------------------
ELASTICSEARCH_URL = "http://localhost:9200"

# ---------------------------------------------------------------------------
# Topic → Elasticsearch index mapping.
# One Kafka topic feeds one Elasticsearch index.
# Index names are lowercase by Elasticsearch convention.
# ---------------------------------------------------------------------------
TOPIC_TO_INDEX = {
    "m9-mysql-customers": "m9-customers",
    "m9-mysql-orders": "m9-orders",
}


def create_consumer() -> KafkaConsumer:
    """
    Build a KafkaConsumer that reads JSON envelopes from both topics.

    bootstrap_servers:
      Entry point for cluster metadata discovery.

    group_id="m9-es-sink":
      All instances of this script that share the same group_id cooperate
      to consume the topics.  Kafka assigns each partition to exactly one
      consumer in the group.  If you run two instances, they split the
      partitions between them.

    auto_offset_reset="earliest":
      When this consumer group has no committed offset (first run, or after
      a reset), start reading from the very beginning of the topic log.
      Use "latest" to only read messages produced after the consumer starts.

    enable_auto_commit=True:
      Kafka automatically commits the consumer's offset after each poll()
      call (every auto_commit_interval_ms, default 5 s).  This is the
      simplest approach but can cause "at-least-once" delivery: if the
      script crashes after poll() but before indexing, the offset is
      committed and those messages are skipped on restart.  For exactly-once,
      commit offsets manually only after successful indexing.

    key_deserializer:
      Converts bytes back to a string.  The producer encoded the key as
      UTF-8 bytes; we reverse that here.  value is None if no key was sent.

    value_deserializer:
      Converts bytes → UTF-8 string → Python dict via json.loads.
      This exactly reverses what json.dumps().encode("utf-8") did in Script 02.
    """
    return KafkaConsumer(
        *TOPIC_TO_INDEX.keys(),  # subscribe to all mapped topics at once
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="m9-es-sink",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        key_deserializer=lambda raw: raw.decode("utf-8") if raw else None,
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
    )


def ensure_index(index_name: str):
    """
    Create the Elasticsearch index if it does not already exist.

    PUT /index_name returns 200 on creation and 400 if it already exists.
    We treat both as success — this is called "idempotent index creation".

    In production you would also supply an explicit mapping (field types),
    but for this lab we rely on Elasticsearch's dynamic mapping.
    """
    url = f"{ELASTICSEARCH_URL}/{index_name}"
    response = requests.put(url, timeout=10)

    if response.status_code == 200:
        print(f"  Created index: {index_name}")
    elif response.status_code == 400:
        # 400 with "resource_already_exists_exception" is expected on restart
        print(f"  Index already exists: {index_name}")
    else:
        response.raise_for_status()


def index_document(topic: str, key: str, envelope: dict):
    """
    Write one Kafka envelope as a document to Elasticsearch.

    We use PUT (not POST) because we supply an explicit document id.
    PUT is idempotent: calling it twice with the same id updates the
    document rather than creating a duplicate.

    Document id = the MySQL row id (extracted from the envelope key or
    the data dict as a fallback).  This means reprocessing the same
    Kafka message produces the same Elasticsearch document — safe to
    replay without creating duplicates.
    """
    index_name = TOPIC_TO_INDEX[topic]
    # The key was serialised as str(id) by the producer.  Fall back to
    # envelope["data"]["id"] in case the key is missing.
    document_id = key if key else str(envelope["data"]["id"])

    url = f"{ELASTICSEARCH_URL}/{index_name}/_doc/{document_id}"
    response = requests.put(url, json=envelope, timeout=10)
    response.raise_for_status()

    action = "created" if response.json().get("result") == "created" else "updated"
    print(
        f"  [{topic}]  id={document_id}  →  {index_name}  ({action})"
    )


def main():
    parser = argparse.ArgumentParser(description="Index Kafka events into Elasticsearch.")
    parser.add_argument(
        "--max-messages",
        type=int,
        default=0,
        help="Stop after N messages (0 = run until Ctrl+C).",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("  Module 9 — Kafka → Elasticsearch Consumer")
    limit_label = f"first {args.max_messages}" if args.max_messages else "unlimited"
    print(f"  Reading: {limit_label} messages | group: m9-es-sink")
    print("=" * 60)

    # ------------------------------------------------------------------
    # Ensure both indexes exist before we start consuming.
    # This is safe to call on every restart.
    # ------------------------------------------------------------------
    for index_name in TOPIC_TO_INDEX.values():
        ensure_index(index_name)

    consumer = create_consumer()
    consumed = 0

    print("\nWaiting for messages ... (Ctrl+C to stop)\n")

    try:
        # consumer.__iter__() calls poll() internally on each iteration.
        # Each iteration yields one ConsumerRecord with attributes:
        #   .topic    — which topic the record came from
        #   .partition — which partition
        #   .offset   — the record's offset in that partition
        #   .key      — the deserialised key (str or None)
        #   .value    — the deserialised value (dict, via our deserialiser)
        for message in consumer:
            try:
                index_document(message.topic, message.key, message.value)
                consumed += 1
            except requests.HTTPError as exc:
                print(f"  ERROR indexing message: {exc}")

            if args.max_messages and consumed >= args.max_messages:
                print(f"\nReached --max-messages {args.max_messages}, stopping.")
                break

    except KeyboardInterrupt:
        print("\nInterrupted by user.")
    except KafkaError as exc:
        print(f"Kafka error: {exc}")
    finally:
        consumer.close()
        print(f"\nConsumer closed.  Total messages indexed: {consumed}")

    print("\nSearch Elasticsearch:")
    print("  curl 'http://localhost:9200/m9-customers/_search?pretty'")
    print("  curl 'http://localhost:9200/m9-orders/_search?pretty'")
    print("\nNext step: run 04_pipeline_monitor.py")


if __name__ == "__main__":
    main()
