#!/usr/bin/env python3
r"""
Module 4 — Script 03: Asynchronous Producer with Delivery Callbacks
====================================================================
Demonstrates the high-throughput production pattern: send without blocking
and receive delivery notifications via callbacks.

Concepts covered:
  - on_delivery callbacks: success callback and error callback
  - How callbacks run on the producer's I/O thread (not the main thread)
  - Batching — the producer accumulates records and sends them together
  - linger_ms — how long to wait before flushing a batch
  - batch_size — maximum bytes per batch
  - Difference between async (callback) and sync (.get()) sending
  - Counting successes and failures across a bulk send

Why async with callbacks?
  Calling .get() after every send() serialises I/O — you wait for broker
  acknowledgement before sending the next message.  With callbacks, the
  producer fills batches in memory and sends them in parallel, achieving
  far higher throughput.  Callbacks notify you of the outcome without
  blocking the sending loop.

Run:
  source ../../.venv/bin/activate          # macOS
  ..\..\..venv\Scripts\Activate.ps1         # Windows PowerShell
  python 03_async_producer_callbacks.py
"""

import json
import threading
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError

TOPIC = "m4-partitioned-topic"

# ---------------------------------------------------------------------------
# Thread-safe counters.
# Callbacks run on the producer's background I/O thread, so we use a
# threading.Lock to protect shared state accessed by the main thread too.
# ---------------------------------------------------------------------------
sent_ok = 0
sent_fail = 0
counter_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Callback functions.
#
# kafka-python invokes on_send_success / on_send_error via
# future.add_callback() / future.add_errback().
# Both are called on the producer's I/O thread.
#
# on_send_success receives a RecordMetadata object.
# on_send_error receives a KafkaError (or subclass) instance.
# ---------------------------------------------------------------------------

def on_send_success(record_metadata):
    """Called when the broker successfully acknowledges the record."""
    global sent_ok
    with counter_lock:
        sent_ok += 1
    # Uncomment the line below to see every ack during the lab:
    # print(f"  ACK  partition={record_metadata.partition}  offset={record_metadata.offset}")


def on_send_error(exc):
    """Called when the record could not be delivered."""
    global sent_fail
    with counter_lock:
        sent_fail += 1
    print(f"  ERROR: {exc}")


# ---------------------------------------------------------------------------
# Construct the producer.
#
# linger_ms=50:
#   The producer waits up to 50 ms before sending a batch, even if
#   batch_size is not reached.  This allows more records to accumulate,
#   improving compression and reducing broker round-trips.
#   The trade-off is a small increase in per-message latency.
#
# batch_size=16384:
#   Maximum bytes in a single batch per partition.  Default is 16 KB.
#   When the batch is full it is sent immediately, regardless of linger_ms.
#
# compression_type="gzip":
#   Compress each batch before sending.  Reduces network bandwidth at the
#   cost of CPU.  Options: None (default), "gzip", "snappy", "lz4", "zstd".
#
# retries=3:
#   If a send fails with a retriable error (e.g. leader election in progress)
#   the client automatically retries up to 3 times before calling the error
#   callback.
# ---------------------------------------------------------------------------
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092", "localhost:9093", "localhost:9094"],
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    key_serializer=lambda key: key.encode("utf-8"),
    acks="all",
    linger_ms=50,
    batch_size=16384,
    compression_type="gzip",
    retries=3,
)

# ---------------------------------------------------------------------------
# Generate a batch of synthetic sensor readings.
# In a real system these might come from a database cursor, an HTTP feed,
# or a file — the producer code stays the same regardless of source.
# ---------------------------------------------------------------------------
NUM_MESSAGES = 100
sensor_ids = ["sensor-A", "sensor-B", "sensor-C"]

print(f"Sending {NUM_MESSAGES} messages to '{TOPIC}' asynchronously ...\n")

start_time = time.time()

for i in range(NUM_MESSAGES):
    sensor_id = sensor_ids[i % len(sensor_ids)]

    payload = {
        "sensor_id": sensor_id,
        "reading": round(20.0 + (i * 0.1), 2),
        "unit": "celsius",
        "sequence": i,
    }

    # -----------------------------------------------------------------------
    # send() is non-blocking.  The record goes into the internal buffer.
    # add_callback() and add_errback() register the functions to call when
    # the broker responds.  Both are registered on the returned Future.
    # -----------------------------------------------------------------------
    (
        producer.send(TOPIC, key=sensor_id, value=payload)
        .add_callback(on_send_success)
        .add_errback(on_send_error)
    )

print("All records enqueued in the internal buffer.")
print("Flushing — waiting for broker acknowledgements ...\n")

# ---------------------------------------------------------------------------
# flush() blocks until every record in the buffer has been either
# acknowledged (triggering on_send_success) or failed permanently
# (triggering on_send_error).
# ---------------------------------------------------------------------------
producer.flush()

elapsed = time.time() - start_time

print(f"Results after {elapsed:.2f}s:")
print(f"  Delivered successfully : {sent_ok}")
print(f"  Failed                 : {sent_fail}")
print(f"  Throughput             : {NUM_MESSAGES / elapsed:.0f} messages/second")

producer.close()

print(f"\nDone.  Open Kafdrop → {TOPIC} to browse the 100 messages.")
print("Notice how sensor-A, sensor-B, sensor-C each land in their own partition.")
