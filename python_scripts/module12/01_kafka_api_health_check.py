#!/usr/bin/env python3
r"""
Module 12 - Script 01: Kafka API Health Check
=============================================
Uses Kafka's Python APIs to inspect cluster metadata, topic offsets, and a
small produce -> consume round trip.

Concepts covered:
  - Admin API: cluster metadata, topic list, topic description
  - Producer API: write keyed JSON records and read RecordMetadata
  - Consumer API: assign(), seek(), poll(), end_offsets()
  - Basic metrics: partition end offsets and round-trip latency

Run:
  # From the repository root:
  source .venv/bin/activate             # macOS / Linux
  .\.venv\Scripts\Activate.ps1          # Windows PowerShell

  python python_scripts/module12/01_kafka_api_health_check.py
  python python_scripts/module12/01_kafka_api_health_check.py --messages 5

Prerequisites:
  - Docker platform running (cd docker && docker compose up -d)
  - Topic m12-metrics-topic exists (bash scripts/create-topics.sh)
"""

import argparse
import json
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer, TopicPartition
from kafka.errors import KafkaError


BOOTSTRAP_SERVERS = ["localhost:9092", "localhost:9093", "localhost:9094"]
TOPIC = "m12-metrics-topic"


def print_header(title: str) -> None:
    print("\n" + "=" * 72)
    print(f"  {title}")
    print("=" * 72)


def describe_cluster(admin: KafkaAdminClient) -> None:
    """Print basic cluster metadata from the Admin API."""
    print_header("Cluster Metadata")
    cluster = admin.describe_cluster()

    controller = cluster.get("controller")
    brokers = cluster.get("brokers", [])

    print(f"Cluster ID : {cluster.get('cluster_id')}")
    print(f"Controller : {controller}")
    print(f"Brokers    : {len(brokers)}")

    for broker in brokers:
        if isinstance(broker, dict):
            broker_id = broker.get("node_id", broker.get("nodeId", broker.get("id")))
            host = broker.get("host")
            port = broker.get("port")
        else:
            broker_id = getattr(broker, "nodeId", getattr(broker, "node_id", None))
            host = getattr(broker, "host", None)
            port = getattr(broker, "port", None)
        print(f"  broker={broker_id} host={host} port={port}")


def describe_topic(admin: KafkaAdminClient, topic: str) -> None:
    """Print topic metadata: partitions, leaders, replicas, ISR."""
    print_header(f"Topic Metadata: {topic}")

    topics = admin.list_topics()
    if topic not in topics:
        print(f"Topic '{topic}' does not exist.")
        return

    description = admin.describe_topics([topic])[0]
    partitions = description.get("partitions", [])

    print(f"Topic      : {topic}")
    print(f"Partitions : {len(partitions)}")

    for partition in partitions:
        partition_id = partition.get("partition")
        leader = partition.get("leader")
        replicas = partition.get("replicas", [])
        isr = partition.get("isr", [])
        print(
            f"  partition={partition_id} leader={leader} "
            f"replicas={replicas} isr={isr}"
        )


def get_end_offsets(topic: str) -> dict[TopicPartition, int]:
    """Return end offsets for every partition in the topic."""
    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=None,
        enable_auto_commit=False,
    )
    try:
        partitions = consumer.partitions_for_topic(topic) or set()
        topic_partitions = [TopicPartition(topic, p) for p in sorted(partitions)]
        consumer.assign(topic_partitions)
        return consumer.end_offsets(topic_partitions)
    finally:
        consumer.close()


def print_end_offsets(topic: str) -> None:
    """Print partition end offsets and total message count."""
    print_header(f"Topic End Offsets: {topic}")
    offsets = get_end_offsets(topic)

    total = 0
    for topic_partition, end_offset in sorted(offsets.items(), key=lambda item: item[0].partition):
        total += end_offset
        print(f"  partition={topic_partition.partition} end_offset={end_offset}")

    print(f"  total_end_offsets={total}")


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=3,
    )


def produce_probe_records(topic: str, count: int) -> list[dict]:
    """Produce count probe records and return metadata needed for exact reads."""
    print_header("Producer API Probe")
    producer = build_producer()
    produced = []

    try:
        for index in range(count):
            event_id = f"m12-{uuid.uuid4().hex[:10]}"
            key = f"probe-{index % 3}"
            value = {
                "event_id": event_id,
                "event_type": "MonitoringProbe",
                "emitted_at": datetime.now(timezone.utc).isoformat(),
                "sequence": index,
            }

            started = time.perf_counter()
            metadata = producer.send(topic, key=key, value=value).get(timeout=15)
            elapsed_ms = (time.perf_counter() - started) * 1000

            item = {
                "event_id": event_id,
                "key": key,
                "partition": metadata.partition,
                "offset": metadata.offset,
                "latency_ms": elapsed_ms,
            }
            produced.append(item)

            print(
                f"OK key={key:<8} event_id={event_id} "
                f"partition={metadata.partition} offset={metadata.offset} "
                f"produce_latency_ms={elapsed_ms:.2f}"
            )

        producer.flush()

    finally:
        producer.close()

    return produced


def consume_exact_records(topic: str, produced: list[dict], timeout_seconds: int) -> None:
    """
    Consume the exact records just produced by assigning partitions and seeking
    to the returned offsets. This avoids disturbing any consumer group.
    """
    print_header("Consumer API Round Trip")

    if not produced:
        print("No produced records to consume.")
        return

    expected_ids = {item["event_id"] for item in produced}
    min_offsets_by_partition = {}
    for item in produced:
        partition = item["partition"]
        min_offsets_by_partition[partition] = min(
            min_offsets_by_partition.get(partition, item["offset"]),
            item["offset"],
        )

    topic_partitions = [
        TopicPartition(topic, partition)
        for partition in sorted(min_offsets_by_partition)
    ]

    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=None,
        enable_auto_commit=False,
        key_deserializer=lambda raw: raw.decode("utf-8") if raw else None,
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
    )

    found = set()
    started = time.perf_counter()
    deadline = started + timeout_seconds

    try:
        consumer.assign(topic_partitions)
        for topic_partition in topic_partitions:
            consumer.seek(topic_partition, min_offsets_by_partition[topic_partition.partition])

        while time.perf_counter() < deadline and found != expected_ids:
            records = consumer.poll(timeout_ms=500)
            for _, messages in records.items():
                for message in messages:
                    event_id = message.value.get("event_id")
                    if event_id in expected_ids and event_id not in found:
                        found.add(event_id)
                        print(
                            f"FOUND event_id={event_id} key={message.key} "
                            f"partition={message.partition} offset={message.offset}"
                        )

        elapsed_ms = (time.perf_counter() - started) * 1000
        print()
        print(f"Expected records : {len(expected_ids)}")
        print(f"Found records    : {len(found)}")
        print(f"Round-trip time  : {elapsed_ms:.2f} ms")

    finally:
        consumer.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Inspect Kafka metadata, offsets, and a produce/consume round trip."
    )
    parser.add_argument(
        "--topic",
        default=TOPIC,
        help=f"Topic to inspect and probe (default: {TOPIC})",
    )
    parser.add_argument(
        "--messages",
        type=int,
        default=3,
        help="Number of probe messages to produce (default: 3)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=10,
        help="Round-trip consume timeout in seconds (default: 10)",
    )
    parser.add_argument(
        "--no-roundtrip",
        action="store_true",
        help="Only inspect metadata and offsets; do not produce/consume probe records.",
    )
    args = parser.parse_args()

    print_header("Module 12 - Kafka API Health Check")
    print(f"Bootstrap : {', '.join(BOOTSTRAP_SERVERS)}")
    print(f"Topic     : {args.topic}")

    admin = KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id="m12-admin-health-check",
        request_timeout_ms=10000,
    )

    try:
        describe_cluster(admin)
        describe_topic(admin, args.topic)
        print_end_offsets(args.topic)

        if not args.no_roundtrip:
            produced = produce_probe_records(args.topic, args.messages)
            consume_exact_records(args.topic, produced, args.timeout)
            print_end_offsets(args.topic)

    except KafkaError as exc:
        print(f"Kafka error: {exc}")
        return 1
    finally:
        admin.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
