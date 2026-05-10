#!/usr/bin/env python3
r"""
Module 5 — Script 01: Simple Consumer
======================================
Demonstrates the minimum code needed to read messages from Kafka.

Concepts covered:
  - Constructing a KafkaConsumer with bootstrap_servers and group_id
  - subscribe() — registering interest in a topic
  - The poll() loop — how consumers request batches of records
  - ConsumerRecord fields: topic, partition, offset, key, value, timestamp
  - auto_offset_reset="earliest" — where to start when no committed offset exists
  - enable_auto_commit — automatic periodic offset committing
  - Deserialising bytes back to Python objects
  - close() — releasing connections and committing final offsets

Flow:
  1. Seed m5-consumer-topic with 12 messages (via an embedded producer)
  2. Create a KafkaConsumer, subscribe to the topic, and poll until all
     seeded messages are received

Run:
  source ../../.venv/bin/activate          # macOS
  ..\..\..venv\Scripts\Activate.ps1         # Windows PowerShell
  python 01_simple_consumer.py

Prerequisites:
  - Docker platform running (cd docker && docker compose up -d)
  - Topic m5-consumer-topic exists (bash scripts/create-topics.sh)
  - Verify in Kafdrop: http://localhost:9000
"""

import json

from kafka import KafkaConsumer, KafkaProducer

BOOTSTRAP_SERVERS = ["localhost:9092", "localhost:9093", "localhost:9094"]
TOPIC = "m5-consumer-topic"
GROUP_ID = "m5-simple-group"
NUM_SEED_MESSAGES = 12   # spread across 3 partitions (4 per partition)


# ---------------------------------------------------------------------------
# Helper — seed the topic so there are always fresh messages to consume.
# ---------------------------------------------------------------------------
def seed_topic(num_messages: int) -> None:
    """Publish num_messages records to TOPIC so the consumer has data to read."""
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )

    print(f"Seeding '{TOPIC}' with {num_messages} messages ...")
    for i in range(num_messages):
        key = f"device-{i % 3}"          # 3 keys → routed to 3 partitions
        value = {"msg_id": i, "sensor": key, "temp": round(20.0 + i * 0.5, 1)}
        producer.send(TOPIC, key=key, value=value)

    producer.flush()
    producer.close()
    print("Seeding complete.\n")


def main():
    # -----------------------------------------------------------------------
    # Step 1 — Seed the topic with fresh messages.
    # -----------------------------------------------------------------------
    seed_topic(NUM_SEED_MESSAGES)

    # -----------------------------------------------------------------------
    # Step 2 — Construct the KafkaConsumer.
    #
    # bootstrap_servers:
    #   Initial contact points for cluster metadata discovery.  The consumer
    #   connects to one of these, downloads broker + partition metadata, and
    #   then talks directly to partition leaders for all subsequent fetches.
    #   Same as the producer — only a subset needs to be reachable.
    #
    # group_id:
    #   A string that names the consumer group this instance belongs to.
    #   Kafka tracks committed offsets per (group_id, topic, partition).
    #   Two consumers with different group_ids read the topic independently —
    #   each gets its own copy of every message.
    #   Re-running this script with the same group_id resumes from the last
    #   committed offset; newly seeded messages are always visible.
    #
    # auto_offset_reset="earliest":
    #   When this group has no committed offset on a partition (first run),
    #   start reading from the very first available record (offset 0).
    #   Use "latest" to only read messages produced after the consumer starts.
    #
    # enable_auto_commit=True (default):
    #   Kafka automatically commits the consumer's reading position to the
    #   internal __consumer_offsets topic every auto_commit_interval_ms
    #   (default 5 000 ms).  On restart, the consumer resumes from the last
    #   committed offset — no messages are re-read.
    #   See Script 03 for manual commit strategies.
    #
    # key_deserializer / value_deserializer:
    #   The inverse of the producer's serialisers.
    #   bytes → UTF-8 string, and bytes → UTF-8 string → Python dict.
    # -----------------------------------------------------------------------
    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        auto_commit_interval_ms=5000,
        key_deserializer=lambda raw: raw.decode("utf-8") if raw else None,
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
    )

    # -----------------------------------------------------------------------
    # Step 3 — Subscribe to the topic.
    #
    # subscribe() registers this consumer's interest in a topic.  The broker
    # will assign partitions to this consumer as part of the group protocol
    # (JoinGroup → SyncGroup requests).  Partition assignment is NOT complete
    # until after the first poll() call — the assignment happens lazily.
    #
    # You can subscribe to multiple topics at once:
    #   consumer.subscribe(["topic-a", "topic-b"])
    # Or to a pattern (regex):
    #   consumer.subscribe(pattern="^m5-.*")
    #
    # subscribe() and assign() are mutually exclusive.  See Script 04 for
    # the assign() approach (standalone consumer, no group protocol).
    # -----------------------------------------------------------------------
    consumer.subscribe([TOPIC])

    print(f"Consumer subscribed to '{TOPIC}'")
    print(f"Group ID : {GROUP_ID}")
    print(f"Polling until {NUM_SEED_MESSAGES} messages are received ...\n")
    print(f"  {'Topic':<22} {'Part':>4}  {'Offset':>6}  {'Key':<12}  Value")
    print(f"  {'-'*22} {'-'*4}  {'-'*6}  {'-'*12}  -----")

    # -----------------------------------------------------------------------
    # Step 4 — The poll loop.
    #
    # consumer.poll(timeout_ms) requests records from the broker across all
    # assigned partitions and returns a dict:
    #   { TopicPartition(topic, partition) → [ConsumerRecord, ...] }
    #
    # If no records are available within timeout_ms, the dict is empty.
    #
    # ConsumerRecord fields:
    #   .topic      — topic name string
    #   .partition  — partition number (0-based integer)
    #   .offset     — position in the partition log (monotonically increasing)
    #   .key        — deserialized key (None if the message had no key)
    #   .value      — deserialized value
    #   .timestamp  — producer-side Unix timestamp in milliseconds
    #
    # In a production service this loop runs forever.  Here we stop after
    # receiving NUM_SEED_MESSAGES or after 5 consecutive empty polls.
    # -----------------------------------------------------------------------
    received = 0
    empty_polls = 0

    while received < NUM_SEED_MESSAGES:
        records = consumer.poll(timeout_ms=2000)

        if not records:
            empty_polls += 1
            if empty_polls >= 5:
                print("  No records after 5 empty polls — stopping.")
                break
            continue

        empty_polls = 0   # reset counter on any non-empty poll

        for tp, messages in records.items():
            for msg in messages:
                print(
                    f"  {msg.topic:<22} {msg.partition:>4}  {msg.offset:>6}"
                    f"  {str(msg.key):<12}  {msg.value}"
                )
                received += 1

    print(f"\nTotal messages received: {received}")

    # -----------------------------------------------------------------------
    # Step 5 — Close the consumer.
    #
    # close() does three things:
    #   1. Performs a final synchronous offset commit (even with auto-commit)
    #   2. Sends a LeaveGroup request so the broker immediately reassigns
    #      this consumer's partitions to remaining group members
    #   3. Releases all network connections and background threads
    #
    # Without close() the broker waits for the session timeout (default 10 s)
    # before detecting the departure and triggering a rebalance.
    #
    # In production, prefer using the consumer as a context manager so
    # close() is guaranteed even on exceptions:
    #   with KafkaConsumer(...) as consumer:
    #       for msg in consumer:
    #           process(msg)
    # -----------------------------------------------------------------------
    consumer.close()
    print("Consumer closed.")
    print(f"\nOpen Kafdrop → {TOPIC} → Consumers to see the committed group offset.")
    print(f"Group '{GROUP_ID}' offsets are stored in __consumer_offsets.")


if __name__ == "__main__":
    main()
