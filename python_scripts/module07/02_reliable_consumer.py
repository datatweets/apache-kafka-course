#!/usr/bin/env python3
r"""
Module 7 - Script 02: Reliable Consumer
=======================================
Demonstrates a consumer pattern for at-least-once processing.

Concepts covered:
  - enable_auto_commit=False - application controls offset commits
  - Process first, commit after successful processing
  - Idempotent destination writes using an "UPSERT" style sink
  - Re-delivery demonstration with --fail-after N

Run:
  # From the repository root:
  source .venv/bin/activate             # macOS / Linux
  .\.venv\Scripts\Activate.ps1          # Windows PowerShell

  # Terminal 1: produce records
  python python_scripts/module07/01_reliable_producer.py --count 8

  # Terminal 2: consume and commit after processing
  python python_scripts/module07/02_reliable_consumer.py --max-messages 8

  # Demonstrate re-delivery:
  python python_scripts/module07/01_reliable_producer.py --count 4
  python python_scripts/module07/02_reliable_consumer.py --group-id m7-fail-demo --fail-after 2
  python python_scripts/module07/02_reliable_consumer.py --group-id m7-fail-demo --max-messages 4

Prerequisites:
  - Docker platform running (cd docker && docker compose up -d)
  - Topic m7-reliable-topic exists (bash scripts/create-topics.sh)
"""

import argparse
import json

from kafka import KafkaConsumer


BOOTSTRAP_SERVERS = ["localhost:9092", "localhost:9093", "localhost:9094"]
TOPIC = "m7-reliable-topic"
GROUP_ID = "m7-reliable-consumer"


def build_consumer(topic: str, group_id: str, max_poll_records: int) -> KafkaConsumer:
    """Create a consumer that commits offsets only when the script says so."""
    return KafkaConsumer(
        topic,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        max_poll_records=max_poll_records,
        key_deserializer=lambda raw: raw.decode("utf-8") if raw else None,
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
    )


def upsert_to_sink(sink: dict, key: str, event: dict) -> None:
    """
    Simulate an idempotent destination write.

    A real sink might be Elasticsearch, MySQL, Redis, or object storage.
    Here, assigning by key models an upsert: processing the same message again
    overwrites the same logical record instead of creating a duplicate.
    """
    sink[key] = {
        "event_id": event.get("event_id"),
        "event_type": event.get("event_type"),
        "data": event.get("data", {}),
    }


def format_event_summary(event: dict) -> str:
    data = event.get("data", {})
    return (
        f"event_id={event.get('event_id')} "
        f"order_id={data.get('order_id')} "
        f"status={data.get('status')}"
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Consume records with manual commits after idempotent processing."
    )
    parser.add_argument(
        "--topic",
        default=TOPIC,
        help=f"Topic to consume (default: {TOPIC})",
    )
    parser.add_argument(
        "--group-id",
        default=GROUP_ID,
        help=f"Consumer group id (default: {GROUP_ID})",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=10,
        help="Stop after this many processed messages (default: 10)",
    )
    parser.add_argument(
        "--max-poll-records",
        type=int,
        default=5,
        help="Maximum records per poll batch (default: 5)",
    )
    parser.add_argument(
        "--fail-after",
        type=int,
        default=None,
        metavar="N",
        help="Simulate a crash after processing N messages but before commit",
    )
    args = parser.parse_args()

    print("=" * 72)
    print("  Module 7 - Reliable Consumer")
    print("=" * 72)
    print(f"  Topic       : {args.topic}")
    print(f"  Group ID    : {args.group_id}")
    print("  Auto commit : disabled")
    print("  Commit rule : commit only after successful processing")
    if args.fail_after is not None:
        print(f"  Failure demo: crash before commit after {args.fail_after} message(s)")
    print("=" * 72)
    print()

    consumer = build_consumer(args.topic, args.group_id, args.max_poll_records)
    sink = {}
    processed = 0
    empty_polls = 0
    simulated_failure = False

    try:
        while processed < args.max_messages and empty_polls < 5:
            records = consumer.poll(timeout_ms=2000)

            if not records:
                empty_polls += 1
                print(f"No records available ({empty_polls}/5 empty polls).")
                continue

            empty_polls = 0
            batch_count = 0

            for topic_partition, messages in records.items():
                for message in messages:
                    if processed >= args.max_messages:
                        break

                    sink_key = message.key or (
                        f"{message.topic}-{message.partition}-{message.offset}"
                    )
                    upsert_to_sink(sink, sink_key, message.value)
                    processed += 1
                    batch_count += 1

                    print(
                        f"UPSERT sink[{sink_key}] "
                        f"partition={message.partition} offset={message.offset} "
                        f"{format_event_summary(message.value)}"
                    )

                    if args.fail_after is not None and processed >= args.fail_after:
                        simulated_failure = True
                        print()
                        print("Simulated crash now: processed records are NOT committed.")
                        print("Run the same group again to observe re-delivery.")
                        return 1

            if batch_count:
                consumer.commit()
                print(f"Committed offsets after processing {batch_count} record(s).\n")

    finally:
        # Do not auto-commit during close. We only commit explicitly after
        # successful processing above.
        consumer.close(autocommit=False)

    print("=" * 72)
    print(f"  Processed records : {processed}")
    print(f"  Sink records      : {len(sink)}")
    print("=" * 72)

    if simulated_failure:
        return 1

    print("Finished. Offsets were committed only after successful processing.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
