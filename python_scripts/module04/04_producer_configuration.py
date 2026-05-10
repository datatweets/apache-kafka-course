#!/usr/bin/env python3
r"""
Module 4 — Script 04: Producer Configuration Deep Dive
=======================================================
Demonstrates every significant producer configuration parameter, grouped
by category.  Run each CONFIG_PROFILE to observe the behavioural difference.

Concepts covered:
  - acks (0, 1, "all") and the durability/throughput trade-off
  - retries, retry_backoff_ms, and delivery.timeout.ms
  - Idempotent producer — exactly-once delivery within a single session
  - Batching: linger_ms, batch_size
  - Compression: gzip, snappy, lz4, zstd
  - max_in_flight_requests_per_connection — ordering vs throughput
  - request_timeout_ms and delivery_timeout_ms
  - buffer_memory — what happens when the buffer fills
  - max_block_ms — how long send() blocks when the buffer is full

Usage:
  source ../../.venv/bin/activate          # macOS
  ..\..\..venv\Scripts\Activate.ps1         # Windows PowerShell
  python 04_producer_configuration.py --profile throughput
  python 04_producer_configuration.py --profile reliable
  python 04_producer_configuration.py --profile idempotent
  python 04_producer_configuration.py --profile latency
"""

import argparse
import json
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError

TOPIC = "m4-partitioned-topic"
NUM_MESSAGES = 50

# ---------------------------------------------------------------------------
# Configuration profiles.
# Each profile is a dict that maps directly to KafkaProducer keyword args.
# The "description" key is for human display only and removed before init.
# ---------------------------------------------------------------------------
PROFILES = {

    # -----------------------------------------------------------------------
    # THROUGHPUT profile
    # Goal: maximise messages per second at the cost of some durability.
    #
    # acks=1         — only the partition leader must write to its log before
    #                  acknowledging.  Followers do not need to confirm.
    #                  If the leader crashes before replication, the message
    #                  is lost.
    #
    # linger_ms=20   — wait up to 20 ms to accumulate more records into each
    #                  batch, reducing the number of network round-trips.
    #
    # batch_size     — 32 KB per partition batch (default is 16 KB).
    #
    # compression_type="lz4" — fast compression, good ratio.  Reduces network
    #                  bytes at very low CPU cost.
    #
    # max_in_flight_requests_per_connection=5
    #                — allow 5 unacknowledged requests to the broker at once.
    #                  Higher values increase throughput but can reorder
    #                  messages if retries occur (see idempotent profile).
    # -----------------------------------------------------------------------
    "throughput": {
        "description": "Maximise throughput — some durability trade-off",
        "acks": 1,
        "linger_ms": 20,
        "batch_size": 32768,
        "compression_type": "lz4",
        "max_in_flight_requests_per_connection": 5,
    },

    # -----------------------------------------------------------------------
    # RELIABLE profile
    # Goal: guarantee no data loss, at the cost of higher latency.
    #
    # acks="all"     — the broker waits for ALL in-sync replicas (ISR) to
    #                  write the record before acknowledging.  Even if the
    #                  leader dies, any surviving replica has the data.
    #
    # retries=10     — retry up to 10 times on retriable errors
    #                  (e.g. NOT_LEADER_FOR_PARTITION during an election).
    #
    # retry_backoff_ms=300
    #                — wait 300 ms between retries to avoid hammering a
    #                  temporarily unavailable broker.
    #
    # max_in_flight_requests_per_connection=1
    #                — send only one in-flight request at a time.  This
    #                  prevents message reordering when retries occur.
    #                  (Use idempotent=True for ordering with > 1 in-flight.)
    # -----------------------------------------------------------------------
    "reliable": {
        "description": "Maximise durability — no data loss, higher latency",
        "acks": "all",
        "retries": 10,
        "retry_backoff_ms": 300,
        "max_in_flight_requests_per_connection": 1,
        "linger_ms": 5,
        "compression_type": "gzip",
    },

    # -----------------------------------------------------------------------
    # IDEMPOTENT profile
    # Goal: exactly-once delivery within a single producer session.
    #
    # enable_idempotence=True
    #                — the broker assigns this producer a unique Producer ID
    #                  (PID) and sequence numbers each record per partition.
    #                  If the producer retries a failed send, the broker
    #                  deduplicates using the PID + sequence number, so the
    #                  record is written exactly once.
    #
    # When enable_idempotence=True the following are automatically set
    # (you still spell them out here for learning clarity):
    #   acks="all"
    #   retries >= 1
    #   max_in_flight_requests_per_connection = 1
    #
    # Note: kafka-python requires max_in_flight_requests_per_connection=1
    # when enable_idempotence=True (the Java client allows up to 5, but
    # kafka-python v2.x enforces the stricter constraint).
    #
    # Note: idempotence is per-session.  If the producer process restarts,
    # a new PID is assigned.  For cross-session exactly-once you need
    # Kafka Transactions (covered in Module 7).
    # -----------------------------------------------------------------------
    "idempotent": {
        "description": "Idempotent producer — exactly-once per session",
        "enable_idempotence": True,
        "acks": "all",
        "retries": 5,
        "max_in_flight_requests_per_connection": 1,  # kafka-python requires 1 with idempotence
        "linger_ms": 10,
    },

    # -----------------------------------------------------------------------
    # LATENCY profile
    # Goal: minimise the time between send() and broker acknowledgement.
    #
    # linger_ms=0    — send each record immediately, do not wait to batch.
    #                  Every send() creates a single-record network request.
    #                  Best latency, worst throughput.
    #
    # batch_size=1   — effectively disables batching.
    #
    # compression_type=None — skip compression to remove encoding CPU cost.
    #
    # acks=1         — wait for leader only; fastest acknowledgement.
    # -----------------------------------------------------------------------
    "latency": {
        "description": "Minimise latency — one record per request",
        "acks": 1,
        "linger_ms": 0,
        "batch_size": 1,
        "compression_type": None,
        "max_in_flight_requests_per_connection": 1,
    },
}


def build_producer(profile_name: str) -> KafkaProducer:
    """Construct a KafkaProducer from the named profile."""
    config = dict(PROFILES[profile_name])
    description = config.pop("description")  # not a valid KafkaProducer kwarg

    print(f"\nProfile  : {profile_name}")
    print(f"Goal     : {description}")
    print("Settings :")
    for key, val in config.items():
        print(f"  {key} = {val!r}")
    print()

    # Fixed settings common to all profiles:
    #
    # buffer_memory=33554432 (32 MB, the default):
    #   Total bytes the producer can buffer across all partitions before
    #   send() blocks (controlled by max_block_ms).  If your producer
    #   generates faster than the broker can accept, this buffer fills up.
    #
    # max_block_ms=60000 (60 s, the default):
    #   How long send() or partitions_for() will block when the buffer is
    #   full or metadata is unavailable before raising BufferError.
    #
    # request_timeout_ms=30000 (30 s, the default):
    #   How long the producer waits for a response from the broker for a
    #   single network request before considering it failed.

    return KafkaProducer(
        bootstrap_servers=["localhost:9092", "localhost:9093", "localhost:9094"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        buffer_memory=33554432,        # 32 MB — default, shown explicitly
        max_block_ms=60000,            # 60 s  — default, shown explicitly
        request_timeout_ms=30000,      # 30 s  — default, shown explicitly
        **config,
    )


def send_batch(producer: KafkaProducer, num_messages: int) -> dict:
    """Send num_messages records and return timing + outcome stats."""
    success_count = 0
    error_count = 0

    start = time.perf_counter()

    for i in range(num_messages):
        payload = {
            "event_id": i,
            "sensor": f"sensor-{i % 3}",
            "value": round(22.0 + i * 0.05, 3),
        }
        key = f"sensor-{i % 3}"

        try:
            # .get(timeout=15) makes this synchronous so we measure the
            # full round-trip latency per profile accurately.
            producer.send(TOPIC, key=key, value=payload).get(timeout=15)
            success_count += 1
        except KafkaError as exc:
            error_count += 1
            print(f"  Send error (msg {i}): {exc}")

    elapsed = time.perf_counter() - start

    return {
        "sent": num_messages,
        "success": success_count,
        "errors": error_count,
        "elapsed_s": round(elapsed, 3),
        "msg_per_sec": round(num_messages / elapsed, 1),
        "avg_latency_ms": round((elapsed / num_messages) * 1000, 2),
    }


def main():
    parser = argparse.ArgumentParser(
        description="Demonstrate Kafka producer configuration profiles."
    )
    parser.add_argument(
        "--profile",
        choices=list(PROFILES.keys()),
        default="reliable",
        help="Which configuration profile to run (default: reliable)",
    )
    args = parser.parse_args()

    producer = build_producer(args.profile)

    print(f"Sending {NUM_MESSAGES} messages to '{TOPIC}' ...\n")
    stats = send_batch(producer, NUM_MESSAGES)
    producer.flush()
    producer.close()

    print("=" * 50)
    print(f"Profile          : {args.profile}")
    print(f"Messages sent    : {stats['sent']}")
    print(f"Delivered OK     : {stats['success']}")
    print(f"Errors           : {stats['errors']}")
    print(f"Total time       : {stats['elapsed_s']} s")
    print(f"Throughput       : {stats['msg_per_sec']} msg/s")
    print(f"Avg latency      : {stats['avg_latency_ms']} ms/msg")
    print("=" * 50)
    print()
    print("Tip: run all four profiles and compare the latency column:")
    print("  python 04_producer_configuration.py --profile throughput")
    print("  python 04_producer_configuration.py --profile reliable")
    print("  python 04_producer_configuration.py --profile idempotent")
    print("  python 04_producer_configuration.py --profile latency")


if __name__ == "__main__":
    main()
