"""
Module 13 — Script 01: Stream Producer
=======================================
Course: Apache Kafka (2-Day, Instructor-Led)
Module: 13 — Stream Processing

PURPOSE
-------
Produces a continuous stream of sentence events to two input topics:

  m13-stream-input      used by the stateless transform (script 02)
  m13-wordcount-input   used by the stateful word count (script 03)

Run this script first in its own terminal, then start scripts 02 and/or
03 in separate terminals to observe live stream processing.

HOW TO RUN
----------
  # Activate the virtual environment (from repository root):
  source ../../.venv/bin/activate          # macOS
  ..\\..\\..venv\\Scripts\\Activate.ps1         # Windows PowerShell

  python 01_stream_producer.py

WHAT TO EXPECT
--------------
  Module 13 — Stream Producer
  Producing to m13-stream-input and m13-wordcount-input every 2 seconds
  Press Ctrl+C to stop.

  [   1]  "Apache Kafka is a distributed event streaming platform"
  [   2]  "Stream processing transforms events as they arrive"
  ...

TOPICS USED
-----------
  m13-stream-input      (write)
  m13-wordcount-input   (write)
"""

import itertools
import time

from kafka import KafkaProducer

BOOTSTRAP_SERVERS = ["localhost:9092", "localhost:9093", "localhost:9094"]

# Sentences that demonstrate meaningful word-count variation and
# reference real stream-processing concepts learners just covered.
SENTENCES = [
    "Apache Kafka is a distributed event streaming platform",
    "Stream processing transforms events as they arrive in real time",
    "Kafka topics store events in an ordered and durable log",
    "Producers write events and consumers read events from topics",
    "Faust is a Python stream processing library built on Kafka",
    "Stream processing enables real time analytics without batch delays",
    "Word count is the hello world of stream processing systems",
    "Kafka supports both stateless and stateful stream processing",
    "Events flow through agents and are transformed continuously",
    "Kafka Streams and Faust both build on Kafka topics and partitions",
    "Stateless processing transforms each event independently",
    "Stateful processing accumulates results across many events",
    "Windows group events by time to compute bounded aggregations",
    "At least once delivery means every event is processed one or more times",
    "Idempotent writes make duplicate processing safe for the destination",
]


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: v.encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )

    print("\n  Module 13 — Stream Producer")
    print("  Producing to m13-stream-input and m13-wordcount-input every 2 seconds")
    print("  Press Ctrl+C to stop.\n")

    try:
        for count, sentence in enumerate(itertools.cycle(SENTENCES), start=1):
            key = f"sentence-{count}"
            producer.send("m13-stream-input", key=key, value=sentence)
            producer.send("m13-wordcount-input", key=key, value=sentence)
            print(f"  [{count:>4}]  \"{sentence}\"")
            time.sleep(2)

    except KeyboardInterrupt:
        print("\n  Stopped by user.")
    finally:
        producer.flush()
        producer.close()
        print("  Producer closed.\n")


if __name__ == "__main__":
    main()
