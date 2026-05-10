#!/usr/bin/env python3
r"""
Module 5 - Script 04: Standalone Consumer (Manual Partition Assignment)
========================================================================
Demonstrates reading from specific partitions using assign() instead of
subscribe() - bypassing the consumer group protocol entirely.

Concepts covered:
  - assign() vs subscribe(): the two consumer modes
  - TopicPartition: the (topic, partition) pair that identifies one log
  - Manual partition assignment: no group coordinator, no rebalancing
  - seek(): jump to any specific offset within a partition
  - seek_to_beginning() / seek_to_end(): convenience helpers
  - beginning_offsets() / end_offsets(): query earliest and latest offsets
  - Reading a bounded range: from offset X to offset Y (non-blocking)
  - Use cases: backfill, forensics, replay, testing

When to use a standalone consumer vs a consumer group:

  Consumer group (subscribe):
    - Production services where multiple instances share the work load
    - When Kafka should manage which partitions each instance reads
    - When automatic rebalancing on scale-out/scale-in is desirable

  Standalone consumer (assign):
    - Administrative scripts that inspect specific offsets
    - Replay pipelines that must start from a fixed point
    - Integration tests that need deterministic, non-shared reads
    - Exactly-once pipelines that manage offsets in an external store
      (e.g. in the same database transaction as the processed result)

Run:
  source ../../.venv/bin/activate          # macOS
  ..\..\..venv\Scripts\Activate.ps1         # Windows PowerShell

  python 04_standalone_consumer.py                   # reads all 3 partitions
  python 04_standalone_consumer.py --partitions 0 1  # only partitions 0 and 1
  python 04_standalone_consumer.py --from-offset 2   # start at offset 2
  python 04_standalone_consumer.py --max-messages 5  # stop after 5 messages

Prerequisites:
  - Docker platform running (cd docker && docker compose up -d)
  - Topic m5-consumer-topic exists (bash scripts/create-topics.sh)
  - Run 01_simple_consumer.py first to seed m5-consumer-topic
"""

import argparse
import json

from kafka import KafkaConsumer, KafkaProducer, TopicPartition

BOOTSTRAP_SERVERS = ["localhost:9092", "localhost:9093", "localhost:9094"]
TOPIC = "m5-consumer-topic"
NUM_SEED_MESSAGES = 12


def seed_topic() -> None:
    """Publish NUM_SEED_MESSAGES records to TOPIC (3 partitions)."""
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )
    print(f"Seeding '{TOPIC}' with {NUM_SEED_MESSAGES} messages ...")
    for i in range(NUM_SEED_MESSAGES):
        key = f"device-{i % 3}"
        value = {"msg_id": i, "sensor": key, "temp": round(20.0 + i * 0.5, 1)}
        producer.send(TOPIC, key=key, value=value)
    producer.flush()
    producer.close()
    print("Seeding complete.\n")


def main():
    parser = argparse.ArgumentParser(
        description="Standalone consumer: manually assigned partitions, no group protocol."
    )
    parser.add_argument(
        "--partitions",
        type=int,
        nargs="+",
        default=[0, 1, 2],
        metavar="P",
        help="Which partition numbers to assign (default: 0 1 2)",
    )
    parser.add_argument(
        "--from-offset",
        type=int,
        default=None,
        metavar="N",
        help="Seek each partition to this offset before reading (default: beginning)",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=None,
        metavar="N",
        help="Stop after N messages (default: drain all available records)",
    )
    parser.add_argument(
        "--no-seed",
        action="store_true",
        help="Skip seeding the topic",
    )
    args = parser.parse_args()

    if not args.no_seed:
        seed_topic()

    # ---------------------------------------------------------------------------
    # Build TopicPartition objects for each requested partition.
    #
    # TopicPartition is a named tuple with two fields: topic and partition.
    # It uniquely identifies one partition of one topic and is used across
    # most of the KafkaConsumer API: assign(), seek(), end_offsets(), etc.
    # ---------------------------------------------------------------------------
    topic_partitions = [TopicPartition(TOPIC, p) for p in args.partitions]

    # ---------------------------------------------------------------------------
    # Construct the consumer WITHOUT a group_id.
    #
    # When group_id is omitted (or None):
    #   - No JoinGroup / SyncGroup requests are sent to the broker
    #   - No offset commits are written to __consumer_offsets
    #   - No rebalancing ever occurs - the assignment is entirely ours to manage
    #   - The consumer can still query broker offsets and seek freely
    # ---------------------------------------------------------------------------
    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=None,           # standalone - no group protocol
        enable_auto_commit=False,
        key_deserializer=lambda b: b.decode("utf-8") if b else None,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )

    # ---------------------------------------------------------------------------
    # assign() directly sets which TopicPartitions this consumer reads.
    # Unlike subscribe(), assign() takes effect immediately - no need for
    # a first poll() to trigger the rebalance handshake.
    # ---------------------------------------------------------------------------
    consumer.assign(topic_partitions)

    print("=" * 60)
    print("  Standalone Consumer - Manual Partition Assignment")
    print("=" * 60)
    print(f"  Topic      : {TOPIC}")
    print(f"  Partitions : {args.partitions}")
    print(f"  Group ID   : None  (no group protocol)")

    # ---------------------------------------------------------------------------
    # Query the broker for the current beginning and end offsets of each
    # assigned partition.  This tells us exactly how many records are stored.
    #
    # beginning_offsets(): earliest readable offset (>= 0 for new topics,
    #   may be higher if old segments have been deleted by retention policy)
    # end_offsets(): next offset to be written (i.e. current max offset + 1)
    #   If end_offsets = 10, the latest record is at offset 9.
    # ---------------------------------------------------------------------------
    beginning = consumer.beginning_offsets(topic_partitions)
    end = consumer.end_offsets(topic_partitions)

    print("\n  Partition log state:")
    print(
        f"  {'Partition':>9}  {'Earliest':>8}  {'Next offset':>12}"
        f"  {'Available':>10}"
    )
    print(f"  {'-'*9}  {'-'*8}  {'-'*12}  {'-'*10}")
    for tp in sorted(topic_partitions, key=lambda x: x.partition):
        earliest = beginning[tp]
        latest = end[tp]
        available = max(0, latest - earliest)
        print(
            f"  {tp.partition:>9}  {earliest:>8}  {latest:>12}  {available:>10}"
        )

    # ---------------------------------------------------------------------------
    # Seek: move the fetch position to a specific offset.
    #
    # Option A - seek to a given offset (--from-offset N)
    # Option B - seek to beginning (default: earliest available record)
    #
    # seek() takes effect immediately.  The next poll() fetches from this
    # position onwards.  Unlike subscribe()+auto_offset_reset, seek() always
    # overrides any stored committed offset.
    # ---------------------------------------------------------------------------
    if args.from_offset is not None:
        print(f"\n  Seeking all partitions to offset {args.from_offset} ...")
        for tp in topic_partitions:
            start = max(args.from_offset, beginning[tp])
            consumer.seek(tp, start)
    else:
        print("\n  Seeking all partitions to beginning ...")
        consumer.seek_to_beginning(*topic_partitions)

    # ---------------------------------------------------------------------------
    # Determine how many records are available from the current seek position.
    # We stop when we have read all available records to avoid blocking forever
    # on an empty topic.
    # ---------------------------------------------------------------------------
    total_available = sum(
        max(0, end[tp] - consumer.position(tp))
        for tp in topic_partitions
    )
    max_to_read = (
        min(args.max_messages, total_available)
        if args.max_messages is not None
        else total_available
    )

    print(f"\n  Records available from seek position: {total_available}")
    print(f"  Will read: {max_to_read}")
    print(f"\n  {'Part':>4}  {'Offset':>6}  {'Key':<12}  Value")
    print(f"  {'-'*4}  {'-'*6}  {'-'*12}  -----")

    received = 0
    empty_polls = 0

    while received < max_to_read and empty_polls < 5:
        records = consumer.poll(timeout_ms=2000)
        if not records:
            empty_polls += 1
            continue
        empty_polls = 0
        for tp, messages in records.items():
            for msg in messages:
                if received >= max_to_read:
                    break
                print(
                    f"  {msg.partition:>4}  {msg.offset:>6}"
                    f"  {str(msg.key):<12}  {msg.value}"
                )
                received += 1

    # ---------------------------------------------------------------------------
    # No commit - standalone consumers manage their own offset tracking.
    # A real standalone consumer might store the last-seen offset in a
    # database or config file to resume from the correct position next time.
    # ---------------------------------------------------------------------------
    consumer.close()

    print(f"\n  Total messages read : {received}")
    print("  Offsets NOT committed to __consumer_offsets (no group_id).")
    print("\nKey differences vs subscribe() consumer:")
    print("  assign()    - no group protocol, no rebalancing, partitions fixed")
    print("  subscribe() - group protocol, automatic rebalancing, partitions managed by Kafka")
    print("\nTip: run with different options to explore:")
    print("  python 04_standalone_consumer.py --partitions 0")
    print("  python 04_standalone_consumer.py --from-offset 3")
    print("  python 04_standalone_consumer.py --max-messages 5 --no-seed")


if __name__ == "__main__":
    main()
