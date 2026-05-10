#!/usr/bin/env python3
r"""
Module 12 - Script 02: Consumer Lag Monitor
===========================================
Uses Kafka's Python APIs to calculate consumer group lag for a topic.

Concepts covered:
  - Admin API: list_consumer_group_offsets()
  - Consumer API: end_offsets()
  - Lag calculation: log end offset - committed group offset
  - Watch mode for repeated monitoring

Run:
  # From the repository root:
  source .venv/bin/activate             # macOS / Linux
  .\.venv\Scripts\Activate.ps1          # Windows PowerShell

  python python_scripts/module12/02_consumer_lag_monitor.py \
    --group-id m12-monitor-group \
    --topic m12-metrics-topic

  python python_scripts/module12/02_consumer_lag_monitor.py \
    --group-id m12-monitor-group \
    --topic m12-metrics-topic \
    --watch

Prerequisites:
  - Docker platform running (cd docker && docker compose up -d)
  - Topic m12-metrics-topic exists (bash scripts/create-topics.sh)
  - A consumer group has committed offsets for the topic
"""

import argparse
import time
from datetime import datetime

from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition
from kafka.errors import KafkaError


BOOTSTRAP_SERVERS = ["localhost:9092", "localhost:9093", "localhost:9094"]
TOPIC = "m12-metrics-topic"
GROUP_ID = "m12-monitor-group"


def get_topic_partitions(topic: str) -> list[TopicPartition]:
    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=None,
        enable_auto_commit=False,
    )
    try:
        partitions = consumer.partitions_for_topic(topic) or set()
        return [TopicPartition(topic, p) for p in sorted(partitions)]
    finally:
        consumer.close()


def get_end_offsets(topic_partitions: list[TopicPartition]) -> dict[TopicPartition, int]:
    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=None,
        enable_auto_commit=False,
    )
    try:
        consumer.assign(topic_partitions)
        return consumer.end_offsets(topic_partitions)
    finally:
        consumer.close()


def get_committed_offsets(group_id: str) -> dict[TopicPartition, int]:
    admin = KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id="m12-lag-monitor",
        request_timeout_ms=10000,
    )
    try:
        offsets = admin.list_consumer_group_offsets(group_id)
        committed = {}
        for topic_partition, offset_meta in offsets.items():
            committed[topic_partition] = offset_meta.offset
        return committed
    finally:
        admin.close()


def print_lag(group_id: str, topic: str) -> int:
    topic_partitions = get_topic_partitions(topic)
    if not topic_partitions:
        print(f"Topic '{topic}' does not exist or has no partitions.")
        return 1

    end_offsets = get_end_offsets(topic_partitions)
    committed_offsets = get_committed_offsets(group_id)

    print("=" * 82)
    print(f"  Consumer Lag [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]")
    print("=" * 82)
    print(f"  Group : {group_id}")
    print(f"  Topic : {topic}")
    print()
    print(f"  {'Partition':>9}  {'Committed':>10}  {'Log End':>10}  {'Lag':>10}  Status")
    print(f"  {'-' * 9}  {'-' * 10}  {'-' * 10}  {'-' * 10}  ------")

    total_lag = 0
    missing_offsets = 0

    for topic_partition in topic_partitions:
        end_offset = end_offsets.get(topic_partition, 0)
        committed = committed_offsets.get(topic_partition)

        if committed is None or committed < 0:
            lag = end_offset
            status = "no committed offset"
            committed_label = "-"
            missing_offsets += 1
        else:
            lag = max(0, end_offset - committed)
            status = "caught up" if lag == 0 else "behind"
            committed_label = str(committed)

        total_lag += lag
        print(
            f"  {topic_partition.partition:>9}  {committed_label:>10}  "
            f"{end_offset:>10}  {lag:>10}  {status}"
        )

    print()
    print(f"  Total lag              : {total_lag}")
    print(f"  Partitions without commit: {missing_offsets}")
    print()

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Calculate consumer group lag using Kafka Python APIs."
    )
    parser.add_argument(
        "--topic",
        default=TOPIC,
        help=f"Topic to monitor (default: {TOPIC})",
    )
    parser.add_argument(
        "--group-id",
        default=GROUP_ID,
        help=f"Consumer group id (default: {GROUP_ID})",
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="Refresh repeatedly until Ctrl+C",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=3,
        help="Watch refresh interval in seconds (default: 3)",
    )
    args = parser.parse_args()

    try:
        if args.watch:
            while True:
                print_lag(args.group_id, args.topic)
                time.sleep(args.interval)

        return print_lag(args.group_id, args.topic)

    except KeyboardInterrupt:
        print("\nStopped.")
        return 0
    except KafkaError as exc:
        print(f"Kafka error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
