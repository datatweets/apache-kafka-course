#!/usr/bin/env python3
r"""
Module 4 — Script 02: Keyed Producer with Partition Routing
============================================================
Demonstrates how to use message keys and why they matter.

Concepts covered:
  - What a message key is (and why it can be None)
  - How Kafka's default partitioner uses the key's hash to route messages
  - Guaranteeing that all messages with the same key land in the same partition
  - Serialising both key and value to bytes
  - Reading back RecordMetadata (partition + offset) via Future.get()

Key guarantee:
  All records that share the same key are written to the same partition,
  in order.  This is essential when consumers need to process all events
  for a single entity (e.g. a single customer, order, or device) in order.

Run:
  source ../../.venv/bin/activate          # macOS
  ..\..\..venv\Scripts\Activate.ps1         # Windows PowerShell
  python 02_keyed_producer.py

After running, open Kafdrop → topic m4-keyed-topic → Messages, and observe
that all messages for the same customer_id landed in the same partition.
"""

import json

from kafka import KafkaProducer
from kafka.errors import KafkaError

# ---------------------------------------------------------------------------
# Topic used in this script.
# m4-keyed-topic was created with 3 partitions by create-topics.sh.
# ---------------------------------------------------------------------------
TOPIC = "m4-keyed-topic"

# ---------------------------------------------------------------------------
# Sample data — order events for three customers.
# Notice that customer_id is used as the Kafka key.
# All orders for the same customer will route to the same partition,
# so a consumer processing partition N gets a complete, ordered history
# for every customer assigned to that partition.
# ---------------------------------------------------------------------------
ORDER_EVENTS = [
    {"customer_id": "C001", "order_id": "ORD-100", "amount": 49.99,  "status": "placed"},
    {"customer_id": "C002", "order_id": "ORD-101", "amount": 120.00, "status": "placed"},
    {"customer_id": "C001", "order_id": "ORD-100", "amount": 49.99,  "status": "paid"},
    {"customer_id": "C003", "order_id": "ORD-102", "amount": 15.50,  "status": "placed"},
    {"customer_id": "C002", "order_id": "ORD-101", "amount": 120.00, "status": "shipped"},
    {"customer_id": "C001", "order_id": "ORD-100", "amount": 49.99,  "status": "delivered"},
    {"customer_id": "C003", "order_id": "ORD-102", "amount": 15.50,  "status": "cancelled"},
]

# ---------------------------------------------------------------------------
# Construct the producer with BOTH key_serializer and value_serializer.
#
# key_serializer:
#   Converts the Python key object to bytes.
#   Here the key is a string (customer_id) so we encode it as UTF-8.
#
# value_serializer:
#   Converts the Python dict to JSON bytes.
#   json.dumps() produces a string; encode() converts that string to bytes.
#
# acks="all":
#   The broker waits for all in-sync replicas to confirm the write before
#   acknowledging the producer.  This is the safest durability level.
#   (We explore acks in detail in Script 04.)
# ---------------------------------------------------------------------------
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092", "localhost:9093", "localhost:9094"],
    key_serializer=lambda key: key.encode("utf-8"),
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    acks="all",
)

print(f"Sending {len(ORDER_EVENTS)} events to topic '{TOPIC}'\n")

for event in ORDER_EVENTS:
    # -----------------------------------------------------------------------
    # The key is the customer_id.
    # Kafka's default partitioner computes:
    #   partition = murmur2_hash(key) % num_partitions
    # So the same key always maps to the same partition number.
    # -----------------------------------------------------------------------
    key = event["customer_id"]

    # -----------------------------------------------------------------------
    # producer.send() returns a FutureRecordMetadata.
    # Calling .get(timeout=10) blocks until the broker acknowledges the
    # write (or raises KafkaError after 10 seconds).
    # This is "synchronous" sending — useful for demonstration but
    # slower than batched async sending.  See Script 03 for async delivery.
    # -----------------------------------------------------------------------
    try:
        record_metadata = producer.send(TOPIC, key=key, value=event).get(timeout=10)

        print(
            f"  customer={key} | order={event['order_id']} | status={event['status']}"
            f"  →  partition={record_metadata.partition}  offset={record_metadata.offset}"
        )

    except KafkaError as exc:
        # KafkaError is the base class for all producer-side errors.
        # In production you would log this and decide whether to retry
        # or dead-letter the message.
        print(f"  FAILED to send {event}: {exc}")

# ---------------------------------------------------------------------------
# Always close the producer to flush remaining buffered messages and
# release the network connections cleanly.
# ---------------------------------------------------------------------------
producer.close()

print("\nDone.  Open Kafdrop → m4-keyed-topic to verify partition assignment.")
print("You should see that all C001 events are in the same partition,")
print("all C002 events in the same partition, and so on.")
