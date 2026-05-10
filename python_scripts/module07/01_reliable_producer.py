#!/usr/bin/env python3
r"""
Module 7 - Script 01: Reliable Producer
=======================================
Demonstrates a producer configuration suitable for important data.

Concepts covered:
  - acks="all" - wait for in-sync replicas
  - retries and retry_backoff_ms - recover from transient failures
  - enable_idempotence=True - deduplicate producer retries within a session
  - max_in_flight_requests_per_connection=1 - preserve ordering in kafka-python
  - Keyed JSON records - stable routing and entity-level ordering
  - RecordMetadata - partition and offset confirmation per successful write

Run:
  # From the repository root:
  source .venv/bin/activate             # macOS / Linux
  .\.venv\Scripts\Activate.ps1          # Windows PowerShell

  python python_scripts/module07/01_reliable_producer.py
  python python_scripts/module07/01_reliable_producer.py --count 12

Prerequisites:
  - Docker platform running (cd docker && docker compose up -d)
  - Topic m7-reliable-topic exists (bash scripts/create-topics.sh)
"""

import argparse
import json
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError


BOOTSTRAP_SERVERS = ["localhost:9092", "localhost:9093", "localhost:9094"]
TOPIC = "m7-reliable-topic"


def build_producer() -> KafkaProducer:
    """Create a KafkaProducer with reliable delivery-oriented settings."""
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=10,
        retry_backoff_ms=300,
        enable_idempotence=True,
        max_in_flight_requests_per_connection=1,
    )


def build_event(sequence: int) -> tuple[str, dict]:
    """
    Build a keyed order event.

    The key is stable per order, so all events for the same order are routed
    to the same partition and remain ordered for that order.
    """
    order_id = f"ORD-{1000 + (sequence % 4)}"
    statuses = ["created", "paid", "packed", "shipped"]
    status = statuses[sequence % len(statuses)]

    event = {
        "event_id": f"m7-{sequence:04d}",
        "event_type": "OrderStatusChanged",
        "event_version": 1,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "data": {
            "order_id": order_id,
            "customer_id": f"C{100 + (sequence % 3)}",
            "status": status,
            "amount": round(49.99 + sequence, 2),
        },
    }
    return order_id, event


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Produce reliable keyed JSON records to m7-reliable-topic."
    )
    parser.add_argument(
        "--count",
        type=int,
        default=8,
        help="Number of records to produce (default: 8)",
    )
    parser.add_argument(
        "--topic",
        default=TOPIC,
        help=f"Target topic (default: {TOPIC})",
    )
    args = parser.parse_args()

    print("=" * 68)
    print("  Module 7 - Reliable Producer")
    print("=" * 68)
    print(f"  Topic       : {args.topic}")
    print(f"  Bootstrap   : {', '.join(BOOTSTRAP_SERVERS)}")
    print("  Reliability : acks='all', retries=10, idempotence=True")
    print("  Ordering    : max_in_flight_requests_per_connection=1")
    print("=" * 68)
    print()

    producer = build_producer()
    delivered = 0
    failed = 0

    try:
        for sequence in range(args.count):
            key, event = build_event(sequence)

            try:
                metadata = producer.send(args.topic, key=key, value=event).get(timeout=15)
                delivered += 1
                print(
                    f"OK  key={key:<8} event_id={event['event_id']:<8} "
                    f"partition={metadata.partition} offset={metadata.offset}"
                )
            except KafkaError as exc:
                failed += 1
                print(f"ERR key={key:<8} event_id={event['event_id']:<8} {exc}")

        producer.flush()

    finally:
        producer.close()

    print()
    print("=" * 68)
    print(f"  Delivered : {delivered}")
    print(f"  Failed    : {failed}")
    print("=" * 68)

    if failed:
        print("At least one record failed after retries. Inspect broker/topic health.")
        return 1

    print("All records were acknowledged by Kafka with the reliable producer config.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
