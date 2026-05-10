#!/usr/bin/env python3
r"""
Module 4 — Script 01: Simple Fire-and-Forget Producer
======================================================
Demonstrates the absolute minimum code to construct a KafkaProducer
and send a message to a topic.

Concepts covered:
  - Importing KafkaProducer from the kafka-python library
  - Specifying bootstrap_servers (the entry point into the cluster)
  - Serialising a string value to bytes (Kafka stores raw bytes, not strings)
  - Calling producer.send() — fire-and-forget style
  - Calling producer.flush() to ensure all buffered messages are delivered
    before the script exits

Run:
  source ../../.venv/bin/activate          # macOS
  ..\..\..venv\Scripts\Activate.ps1         # Windows PowerShell
  python 01_simple_producer.py

Prerequisites:
  - Docker platform is running (cd docker && docker compose up -d)
  - Topic m4-simple-topic exists (bash scripts/create-topics.sh)
  - Verify in Kafdrop: http://localhost:9000
"""

from kafka import KafkaProducer

# ---------------------------------------------------------------------------
# Step 1 — Choose the topic to produce to.
# m4-simple-topic was created by create-topics.sh with 3 partitions and
# replication factor 3.  You can see it in Kafdrop at http://localhost:9000.
# ---------------------------------------------------------------------------
TOPIC = "m4-simple-topic"

# ---------------------------------------------------------------------------
# Step 2 — Construct the producer.
#
# bootstrap_servers:
#   A list of "host:port" addresses the client uses to establish the initial
#   connection to the cluster.  It does NOT need to list every broker —
#   Kafka returns full cluster metadata after the first connection.
#
#   Use localhost:9092 / 9093 / 9094 because this script runs on the HOST
#   machine (outside Docker).  These are the PLAINTEXT_HOST listener ports
#   mapped in docker-compose.yml.
#
# value_serializer:
#   Kafka stores raw bytes.  The producer needs to know how to convert your
#   Python object into bytes before sending it over the network.
#   Here we encode a Python string to UTF-8 bytes, which is the simplest
#   possible serialisation.
# ---------------------------------------------------------------------------
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092", "localhost:9093", "localhost:9094"],
    value_serializer=lambda message: message.encode("utf-8"),
)

# ---------------------------------------------------------------------------
# Step 3 — Send a message.
#
# producer.send(topic, value) is non-blocking.  It places the record in an
# internal in-memory buffer and returns a Future.  The actual network I/O
# happens on a background thread managed by the producer.
#
# "Fire-and-forget" means we do not inspect the Future — we trust that
# Kafka will receive it.  This is the highest-throughput, lowest-latency
# approach, but gives no delivery confirmation in application code.
# ---------------------------------------------------------------------------
future = producer.send(TOPIC, value="Hello, Kafka 4 with KRaft!")

print(f"Message enqueued to topic '{TOPIC}' (fire-and-forget)")

# ---------------------------------------------------------------------------
# Step 4 — Flush.
#
# producer.flush() blocks until all buffered messages have been sent and
# acknowledged by the broker.  Without this call, messages sitting in the
# internal buffer could be lost when the script exits and the producer is
# garbage-collected.
#
# Always call flush() (or close()) before your script terminates.
# ---------------------------------------------------------------------------
producer.flush()

print("Flush complete — message has left the producer buffer")

# ---------------------------------------------------------------------------
# Step 5 — Close the producer.
#
# close() performs a final flush and releases network connections.
# In production code use the producer as a context manager (with block)
# to guarantee this happens even on exceptions.
# ---------------------------------------------------------------------------
producer.close()

print("Producer closed.  Check Kafdrop at http://localhost:9000 to see the message.")
