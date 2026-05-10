#!/usr/bin/env python3
r"""
Module 5 — Script 02: Consumer Groups and Partition Assignment
==============================================================
Demonstrates how a consumer group distributes work across multiple consumers
and what happens to partition assignment as consumers are added.

Concepts covered:
  - Consumer group: a set of consumers sharing the same group_id
  - Group coordinator: the broker that manages group membership and assignment
  - Partition assignment: Kafka assigns each partition to exactly ONE consumer
    in the group at any point in time — no two consumers share a partition
  - Rebalancing: triggered when any consumer joins or leaves the group
  - Parallel consumption: N consumers process up to N partitions simultaneously
  - Idle consumers: if num_consumers > num_partitions, extras receive no work
  - RangeAssignor: the default partition assignment strategy (assignable via
    partition_assignment_strategy configuration)

Experiment:
  - Seeds m5-group-topic with 30 messages (3 partitions × 10 messages)
  - Starts N consumer threads, all sharing the same group_id
  - Each thread reports which partitions it was assigned and how many messages
    it received
  - Main thread prints a summary after 8 seconds

Run:
  source ../../.venv/bin/activate          # macOS
  ..\..\..venv\Scripts\Activate.ps1         # Windows PowerShell

  python 02_consumer_groups.py                   # 2 consumers (default)
  python 02_consumer_groups.py --consumers 1
  python 02_consumer_groups.py --consumers 3
  python 02_consumer_groups.py --consumers 4     # one consumer will be idle

Prerequisites:
  - Docker platform running (cd docker && docker compose up -d)
  - Topic m5-group-topic exists with 3 partitions (bash scripts/create-topics.sh)
"""

import argparse
import json
import threading
import time

from kafka import KafkaConsumer, KafkaProducer

BOOTSTRAP_SERVERS = ["localhost:9092", "localhost:9093", "localhost:9094"]
TOPIC = "m5-group-topic"
GROUP_ID = "m5-group-demo"
NUM_SEED_MESSAGES = 30    # 3 partitions × 10 messages each


# ---------------------------------------------------------------------------
# Seed helper
# ---------------------------------------------------------------------------
def seed_topic(num_messages: int) -> None:
    """Seed TOPIC with num_messages records spread across 3 keys/partitions."""
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )
    print(f"Seeding '{TOPIC}' with {num_messages} messages ...")
    for i in range(num_messages):
        key = f"region-{i % 3}"       # 3 keys hash to 3 different partitions
        value = {"event_id": i, "region": key, "payload": f"data-{i}"}
        producer.send(TOPIC, key=key, value=value)
    producer.flush()
    producer.close()
    print("Seeding complete.\n")


# ---------------------------------------------------------------------------
# Consumer worker — runs in its own thread
# ---------------------------------------------------------------------------
def consumer_worker(
    name: str,
    results: dict,
    stop_event: threading.Event,
) -> None:
    """
    Join the consumer group, poll until stop_event is set, then close.

    All workers use the same group_id — this is what makes them a group.
    Kafka's group coordinator assigns each partition to exactly one worker.

    results[name] is written exclusively by this thread, so no locking is
    needed for the dict values (different threads write to different keys).
    """
    # -----------------------------------------------------------------------
    # All consumers in a group share the same group_id.
    # Kafka's group coordinator (one elected broker per group) receives a
    # JoinGroup request from each, elects one as group leader, and instructs
    # the leader to compute the partition assignment.  The leader sends the
    # assignment back to the coordinator (SyncGroup), which distributes it
    # to every member.  This entire process is called the group rebalance.
    # -----------------------------------------------------------------------
    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        key_deserializer=lambda raw: raw.decode("utf-8") if raw else None,
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
    )
    consumer.subscribe([TOPIC])

    results[name] = {
        "partitions_assigned": [],
        "messages_received": 0,
        "by_partition": {},
    }

    while not stop_event.is_set():
        records = consumer.poll(timeout_ms=500)

        # -------------------------------------------------------------------
        # Partition assignment is populated after the first successful poll
        # that completes the rebalance handshake.  We capture it on every
        # iteration so we always reflect the latest (post-rebalance) state.
        # -------------------------------------------------------------------
        assignment = consumer.assignment()
        if assignment:
            results[name]["partitions_assigned"] = sorted(
                tp.partition for tp in assignment
            )

        for tp, messages in records.items():
            for msg in messages:
                results[name]["messages_received"] += 1
                p = msg.partition
                results[name]["by_partition"][p] = (
                    results[name]["by_partition"].get(p, 0) + 1
                )

    consumer.close()


def main():
    parser = argparse.ArgumentParser(
        description="Demonstrate Kafka consumer groups with adjustable parallelism."
    )
    parser.add_argument(
        "--consumers",
        type=int,
        default=2,
        metavar="N",
        help="Number of consumers to run in the group (default: 2)",
    )
    args = parser.parse_args()

    # -----------------------------------------------------------------------
    # Step 1 — Seed the topic with fresh messages.
    # -----------------------------------------------------------------------
    seed_topic(NUM_SEED_MESSAGES)

    # -----------------------------------------------------------------------
    # Step 2 — Launch N consumer threads, all sharing GROUP_ID.
    #
    # Each thread creates its own KafkaConsumer instance.  All instances join
    # the same group.  Kafka treats them as cooperating workers and divides
    # the 3 partitions among them.
    # -----------------------------------------------------------------------
    num_consumers = args.consumers
    print(f"Starting {num_consumers} consumer(s) in group '{GROUP_ID}' ...")
    print(f"Topic '{TOPIC}' has 3 partitions.")
    if num_consumers > 3:
        print(
            f"Note: {num_consumers - 3} consumer(s) will be idle "
            f"(more consumers than partitions)."
        )
    print()

    results = {}
    stop_event = threading.Event()
    threads = []

    for i in range(num_consumers):
        name = f"consumer-{i + 1}"
        t = threading.Thread(
            target=consumer_worker,
            args=(name, results, stop_event),
            name=name,
            daemon=True,
        )
        threads.append(t)
        t.start()

    # -----------------------------------------------------------------------
    # Step 3 — Let consumers run for 8 seconds.
    #
    # 8 s gives the group time to:
    #   - Complete the rebalance (JoinGroup + SyncGroup, usually < 1 s)
    #   - Drain the 30 newly seeded messages
    #   - Fire at least one auto-commit cycle (default 5 s)
    # -----------------------------------------------------------------------
    print("Consuming for 8 seconds ...")
    time.sleep(8)

    # -----------------------------------------------------------------------
    # Step 4 — Signal workers to stop and wait for clean shutdown.
    # -----------------------------------------------------------------------
    stop_event.set()
    for t in threads:
        t.join(timeout=5)

    # -----------------------------------------------------------------------
    # Step 5 — Print the summary.
    # -----------------------------------------------------------------------
    print("\n" + "=" * 60)
    print(f"  Consumer Group Summary  —  group_id='{GROUP_ID}'")
    print("=" * 60)

    total_received = 0
    for name, data in sorted(results.items()):
        assigned = data["partitions_assigned"]
        msgs = data["messages_received"]
        by_part = data["by_partition"]
        total_received += msgs
        print(f"\n  {name}:")
        print(
            f"    Partitions assigned : "
            f"{assigned if assigned else '(none — idle consumer)'}"
        )
        print(f"    Messages received   : {msgs}")
        if by_part:
            print(f"    Per-partition       : {by_part}")

    print(f"\n  Total messages across all consumers: {total_received}")
    print(
        f"  (Newly seeded: {NUM_SEED_MESSAGES} — higher totals include "
        f"messages from previous runs in the same group)"
    )

    print("\nKey observations:")
    if num_consumers == 1:
        print("  → 1 consumer owns all 3 partitions")
    elif num_consumers == 2:
        print("  → 2 consumers, 3 partitions: split is typically [2 partitions] + [1 partition]")
    elif num_consumers == 3:
        print("  → 3 consumers, 3 partitions: perfect 1:1 assignment")
    else:
        print(
            f"  → {num_consumers} consumers, 3 partitions: "
            f"{num_consumers - 3} consumer(s) idle"
        )
    print("  → No message is ever delivered to more than one consumer in the group")
    print("  → Re-run with --consumers 1 / 2 / 3 / 4 to compare assignments")


if __name__ == "__main__":
    main()
