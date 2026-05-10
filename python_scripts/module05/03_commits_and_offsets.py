#!/usr/bin/env python3
r"""
Module 5 — Script 03: Commits and Offsets
==========================================
Explores how Kafka tracks a consumer's reading position and compares four
offset management strategies side by side.

Concepts covered:
  - The offset: a monotonically increasing integer identifying a record's
    position within a partition log
  - __consumer_offsets: the internal Kafka topic where committed offsets are
    stored, keyed by (group_id, topic, partition)
  - enable_auto_commit=True: periodic background commits (simplest, default)
  - Manual synchronous commit: consumer.commit() — blocks until broker confirms
  - Manual asynchronous commit: consumer.commit_async(callback) — non-blocking
  - auto_offset_reset: "earliest" vs "latest" — where to start with no history
  - Seeking: consumer.seek_to_beginning() — replay from offset 0
  - Delivery semantics: at-least-once, at-most-once, and when each applies

Usage:
  source ../../.venv/bin/activate          # macOS
  ..\..\..venv\Scripts\Activate.ps1         # Windows PowerShell

  python 03_commits_and_offsets.py --mode auto_commit
  python 03_commits_and_offsets.py --mode manual_sync
  python 03_commits_and_offsets.py --mode manual_async
  python 03_commits_and_offsets.py --mode seek_to_start

  # Skip re-seeding (messages already exist in the topic):
  python 03_commits_and_offsets.py --mode seek_to_start --no-seed

Prerequisites:
  - Docker platform running (cd docker && docker compose up -d)
  - Topic m5-offsets-topic exists (bash scripts/create-topics.sh)
"""

import argparse
import json

from kafka import KafkaConsumer, KafkaProducer, TopicPartition

BOOTSTRAP_SERVERS = ["localhost:9092", "localhost:9093", "localhost:9094"]
TOPIC = "m5-offsets-topic"
NUM_SEED_MESSAGES = 15


# ---------------------------------------------------------------------------
# Seed helper
# ---------------------------------------------------------------------------
def seed_topic() -> None:
    """Publish NUM_SEED_MESSAGES records to TOPIC."""
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )
    print(f"Seeding '{TOPIC}' with {NUM_SEED_MESSAGES} messages ...")
    for i in range(NUM_SEED_MESSAGES):
        key = f"key-{i % 3}"
        value = {"id": i, "content": f"message-{i}"}
        producer.send(TOPIC, key=key, value=value)
    producer.flush()
    producer.close()
    print("Seeding complete.\n")


# ---------------------------------------------------------------------------
# Mode 1 — Auto-commit (default behaviour)
# ---------------------------------------------------------------------------
def demo_auto_commit() -> None:
    """
    Show how automatic offset committing works.

    Kafka commits the consumer's position every auto_commit_interval_ms
    (default 5 000 ms) on a background timer.  If the process crashes
    between polls, up to 5 seconds of messages may be re-delivered on
    restart — this is at-least-once delivery semantics.

    close() always does a final synchronous commit, so messages consumed
    in the last partial interval are not re-delivered after a clean shutdown.
    """
    print("─" * 60)
    print("MODE: auto_commit")
    print("  enable_auto_commit=True  (commits every 5 s in the background)")
    print("─" * 60)

    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="m5-offsets-auto",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        auto_commit_interval_ms=5000,   # commit every 5 seconds
        key_deserializer=lambda b: b.decode("utf-8") if b else None,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )
    consumer.subscribe([TOPIC])

    received = 0
    empty_polls = 0

    print(f"\n  {'Part':>4}  {'Offset':>6}  {'Key':<10}  Commit strategy")
    print(f"  {'-'*4}  {'-'*6}  {'-'*10}  ---------------")

    while received < NUM_SEED_MESSAGES and empty_polls < 5:
        records = consumer.poll(timeout_ms=2000)
        if not records:
            empty_polls += 1
            continue
        empty_polls = 0
        for tp, messages in records.items():
            for msg in messages:
                print(
                    f"  {msg.partition:>4}  {msg.offset:>6}  {str(msg.key):<10}"
                    f"  auto (fires every 5 s)"
                )
                received += 1

    # close() performs a final synchronous commit before closing connections
    consumer.close()
    print(f"\n  Received: {received}  |  close() committed final offsets.")
    print("  On restart this group resumes from where it left off.")
    print("  Semantics: at-least-once (crash mid-poll = possible re-delivery)\n")


# ---------------------------------------------------------------------------
# Mode 2 — Manual synchronous commit
# ---------------------------------------------------------------------------
def demo_manual_sync() -> None:
    """
    Commit offsets manually after fully processing each batch.

    consumer.commit() is synchronous — it blocks until the broker
    acknowledges the commit request.  This guarantees the offset is
    durably stored before we move to the next batch.

    Typical pattern (at-least-once with controlled checkpoints):
      records = consumer.poll(...)    # 1. fetch batch
      write_to_database(records)      # 2. process / persist
      consumer.commit()               # 3. commit AFTER successful processing

    If the process crashes between steps 2 and 3, the messages are
    re-delivered on restart.  This is at-least-once delivery.
    If you swap steps 2 and 3 (commit before processing), you get
    at-most-once: a crash after commit loses the unprocessed messages.
    """
    print("─" * 60)
    print("MODE: manual_sync")
    print("  enable_auto_commit=False  |  consumer.commit() after each batch")
    print("─" * 60)

    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="m5-offsets-manual-sync",
        auto_offset_reset="earliest",
        enable_auto_commit=False,       # we control every commit
        key_deserializer=lambda b: b.decode("utf-8") if b else None,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )
    consumer.subscribe([TOPIC])

    received = 0
    batch_num = 0
    empty_polls = 0

    print(f"\n  {'Part':>4}  {'Offset':>6}  {'Key':<10}  Status")
    print(f"  {'-'*4}  {'-'*6}  {'-'*10}  ------")

    while received < NUM_SEED_MESSAGES and empty_polls < 5:
        records = consumer.poll(timeout_ms=2000)
        if not records:
            empty_polls += 1
            continue
        empty_polls = 0

        batch_num += 1
        batch_msgs = [msg for messages in records.values() for msg in messages]

        for msg in batch_msgs:
            print(
                f"  {msg.partition:>4}  {msg.offset:>6}  {str(msg.key):<10}"
                f"  processed"
            )
            received += 1

        # -------------------------------------------------------------------
        # Commit AFTER processing the full batch.
        # If we crash here (after processing, before commit), these messages
        # will be re-delivered on restart — at-least-once semantics.
        # consumer.commit() blocks until the broker confirms the commit.
        # -------------------------------------------------------------------
        consumer.commit()
        print(f"  ── batch {batch_num} committed ({len(batch_msgs)} messages) ──")

    consumer.close()
    print(f"\n  Received: {received}  |  All offsets committed manually.")
    print("  Semantics: at-least-once (crash after process, before commit = re-delivery)\n")


# ---------------------------------------------------------------------------
# Mode 3 — Manual asynchronous commit
# ---------------------------------------------------------------------------
def demo_manual_async() -> None:
    """
    Commit offsets without blocking the poll loop.

    consumer.commit_async(callback) fires a commit request in the background.
    The poll loop continues immediately without waiting for broker confirmation.
    The optional callback is invoked (on the consumer's background thread)
    when the broker responds with success or failure.

    Higher throughput than synchronous commit because the poll loop never
    waits for a network round-trip to the broker.

    Best practice: always pair with a final synchronous consumer.commit()
    in a finally block to protect the last batch on clean shutdown.
    """
    print("─" * 60)
    print("MODE: manual_async")
    print("  enable_auto_commit=False  |  commit_async() per batch")
    print("─" * 60)

    def on_commit(offsets, exception):
        """Called on the consumer's background I/O thread when commit completes."""
        if exception:
            print(f"  COMMIT ERROR: {exception}")
        else:
            for tp, meta in offsets.items():
                print(
                    f"  COMMITTED  partition={tp.partition}  offset={meta.offset}"
                )

    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="m5-offsets-manual-async",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        key_deserializer=lambda b: b.decode("utf-8") if b else None,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )
    consumer.subscribe([TOPIC])

    received = 0
    empty_polls = 0

    print(f"\n  {'Part':>4}  {'Offset':>6}  {'Key':<10}")
    print(f"  {'-'*4}  {'-'*6}  {'-'*10}")

    try:
        while received < NUM_SEED_MESSAGES and empty_polls < 5:
            records = consumer.poll(timeout_ms=2000)
            if not records:
                empty_polls += 1
                continue
            empty_polls = 0

            for tp, messages in records.items():
                for msg in messages:
                    print(f"  {msg.partition:>4}  {msg.offset:>6}  {str(msg.key):<10}")
                    received += 1

            # Non-blocking: fires the commit in background, poll loop continues
            consumer.commit_async(callback=on_commit)

    finally:
        # -------------------------------------------------------------------
        # Final synchronous commit on shutdown.
        # This protects the last batch even if commit_async has not yet
        # received broker confirmation before the process exits.
        # -------------------------------------------------------------------
        consumer.commit()
        consumer.close()

    print(f"\n  Received: {received}  |  Final synchronous commit on shutdown.\n")


# ---------------------------------------------------------------------------
# Mode 4 — Seek to beginning (replay from offset 0)
# ---------------------------------------------------------------------------
def demo_seek_to_start() -> None:
    """
    Seek all assigned partitions back to offset 0 and re-read from scratch.

    consumer.seek_to_beginning(*partitions) resets the fetch position on
    the listed TopicPartition objects to the earliest available offset.
    Offsets are NOT committed, so re-running this mode always replays
    from the very beginning.

    Important timing constraint:
      Partition assignment via subscribe() is NOT complete until after the
      first poll() call.  We call poll() once to trigger the JoinGroup /
      SyncGroup handshake, then seek, then poll again from the new position.

    Use cases:
      - Replaying events after a bug fix in downstream processing
      - Populating a new downstream system from the full topic history
      - Testing scenarios that require a clean start on every run
    """
    print("─" * 60)
    print("MODE: seek_to_start")
    print("  Seeks all assigned partitions to offset 0 after joining")
    print("─" * 60)

    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="m5-offsets-seek",
        auto_offset_reset="earliest",
        enable_auto_commit=False,       # never advance the committed offset
        key_deserializer=lambda b: b.decode("utf-8") if b else None,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )
    consumer.subscribe([TOPIC])

    # -----------------------------------------------------------------------
    # First poll — triggers rebalance so partition assignment is available.
    # We discard any records returned here and seek before the next poll.
    # The rebalance may not complete in a single poll call (e.g. when the
    # group coordinator needs to sync all members), so retry until at least
    # one partition is assigned.
    # -----------------------------------------------------------------------
    assigned = set()
    for _ in range(10):
        consumer.poll(timeout_ms=1000)
        assigned = consumer.assignment()
        if assigned:
            break

    print(f"\n  Assigned partitions: {sorted(tp.partition for tp in assigned)}")

    # -----------------------------------------------------------------------
    # seek_to_beginning() moves the fetch cursor on every listed partition
    # to its earliest available offset (usually 0, but could be higher if
    # old segments have been deleted due to retention policy).
    # -----------------------------------------------------------------------
    consumer.seek_to_beginning(*assigned)
    print("  Seeked all partitions to offset 0.")
    print("  Re-reading from the start ...\n")
    print(f"  {'Part':>4}  {'Offset':>6}  {'Key':<10}  Value")
    print(f"  {'-'*4}  {'-'*6}  {'-'*10}  -----")

    received = 0
    empty_polls = 0

    while received < NUM_SEED_MESSAGES and empty_polls < 5:
        records = consumer.poll(timeout_ms=2000)
        if not records:
            empty_polls += 1
            continue
        empty_polls = 0
        for tp, messages in records.items():
            for msg in messages:
                print(
                    f"  {msg.partition:>4}  {msg.offset:>6}"
                    f"  {str(msg.key):<10}  {msg.value}"
                )
                received += 1

    consumer.close()
    print(f"\n  Re-read {received} messages from offset 0.")
    print("  Offsets NOT committed — run this mode again to re-read the same messages.\n")


# ---------------------------------------------------------------------------
# Dispatch table
# ---------------------------------------------------------------------------
MODES = {
    "auto_commit":   demo_auto_commit,
    "manual_sync":   demo_manual_sync,
    "manual_async":  demo_manual_async,
    "seek_to_start": demo_seek_to_start,
}


def main():
    parser = argparse.ArgumentParser(
        description="Demonstrate Kafka consumer offset and commit strategies."
    )
    parser.add_argument(
        "--mode",
        choices=list(MODES.keys()),
        default="auto_commit",
        help="Which commit/seek strategy to demonstrate (default: auto_commit)",
    )
    parser.add_argument(
        "--no-seed",
        action="store_true",
        help="Skip seeding the topic (messages already exist)",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("  Module 5 — Commits and Offsets")
    print("=" * 60)

    if not args.no_seed:
        seed_topic()

    MODES[args.mode]()

    print("Tip: run all four modes and compare behaviour:")
    print("  python 03_commits_and_offsets.py --mode auto_commit")
    print("  python 03_commits_and_offsets.py --mode manual_sync")
    print("  python 03_commits_and_offsets.py --mode manual_async")
    print("  python 03_commits_and_offsets.py --mode seek_to_start --no-seed")


if __name__ == "__main__":
    main()
