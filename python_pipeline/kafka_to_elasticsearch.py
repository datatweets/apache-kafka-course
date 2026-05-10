#!/usr/bin/env python3
"""
Consume JSON events from Kafka and index them into Elasticsearch.

This is a teaching example for the course pipeline:
MySQL -> Kafka -> Elasticsearch
"""

import argparse
import json

import requests
from kafka import KafkaConsumer


KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092", "localhost:9093", "localhost:9094"]
ELASTICSEARCH_URL = "http://localhost:9200"

TOPIC_TO_INDEX = {
    "python.mysql.customers": "python-customers",
    "python.mysql.orders": "python-orders",
}


def create_consumer():
    return KafkaConsumer(
        *TOPIC_TO_INDEX.keys(),
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="python-elasticsearch-sink",
        key_deserializer=lambda value: value.decode("utf-8") if value else None,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )


def ensure_index(index_name):
    response = requests.put(f"{ELASTICSEARCH_URL}/{index_name}", timeout=10)
    if response.status_code not in (200, 400):
        response.raise_for_status()


def index_event(topic, key, event):
    index_name = TOPIC_TO_INDEX[topic]
    document_id = key or event["data"]["id"]
    response = requests.put(
        f"{ELASTICSEARCH_URL}/{index_name}/_doc/{document_id}",
        json=event,
        timeout=10,
    )
    response.raise_for_status()
    print(f"indexed topic={topic} id={document_id} into {index_name}")


def main():
    parser = argparse.ArgumentParser(description="Index Kafka events into Elasticsearch.")
    parser.add_argument("--max-messages", type=int, default=0, help="Stop after N messages. 0 means run forever.")
    args = parser.parse_args()

    for index_name in TOPIC_TO_INDEX.values():
        ensure_index(index_name)

    consumer = create_consumer()
    consumed = 0

    try:
        for message in consumer:
            index_event(message.topic, message.key, message.value)
            consumed += 1
            if args.max_messages and consumed >= args.max_messages:
                break
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
