"""
Module 13 — Script 02: Stateless Stream Transform
==================================================
Course: Apache Kafka (2-Day, Instructor-Led)
Module: 13 — Stream Processing

PURPOSE
-------
Demonstrates STATELESS stream processing using Faust.

A Faust "agent" is an async coroutine that processes events one at a time
as they arrive on an input topic. Each event is handled independently —
no memory of previous events is kept. This is stateless transformation.

This agent:
  1. Reads a raw sentence from m13-stream-input
  2. Enriches it: uppercase, word count, character count, timestamp
  3. Writes the enriched JSON event to m13-stream-output

KEY CONCEPT — Stateless:
  The output depends only on the current input event.
  If the worker restarts, it simply continues from where it left off.
  No accumulated state is lost because there is none.

HOW TO RUN
----------
  Terminal 1 — start the producer:
    python 01_stream_producer.py

  Terminal 2 — start this Faust worker (from repository root activate venv):
    source ../../.venv/bin/activate          # macOS
    ..\\..\\..venv\\Scripts\\Activate.ps1         # Windows PowerShell

    python 02_stateless_transform.py worker -l info

  Terminal 3 — monitor output (optional):
    python 04_stream_monitor.py

WHAT TO EXPECT
--------------
  The Faust worker logs startup, connects to Kafka, and then prints a
  line for each transformed event:

    [transform] "Apache Kafka is a distributed event..." -> words=8, chars=53

  The enriched JSON is written to m13-stream-output.

FAUST CONCEPTS INTRODUCED
--------------------------
  faust.App      the application; holds broker config, app ID, defaults
  @app.agent()   decorates an async generator that consumes a topic stream
  app.topic()    declares a Kafka topic Faust will read from or write to
  async for      Faust streams are async generators — the loop is non-blocking
  await send()   produces an event to an output topic asynchronously

TOPICS USED
-----------
  m13-stream-input    (read)
  m13-stream-output   (write)
"""

import json
from datetime import datetime, timezone

import faust

# ── Faust Application ─────────────────────────────────────────────────────────
# The app ID becomes the Kafka consumer group prefix and the prefix for any
# internal Faust topics.  store="memory://" avoids a RocksDB dependency — fine
# for learning; in production use store="rocksdb://" for persistence.
app = faust.App(
    "m13-stateless-transform",
    broker="kafka://localhost:9092;localhost:9093;localhost:9094",
    value_serializer="raw",   # treat all topic values as raw bytes by default
    store="memory://",
)

# ── Topic Declarations ────────────────────────────────────────────────────────
input_topic = app.topic("m13-stream-input", value_type=bytes)
output_topic = app.topic("m13-stream-output", value_type=bytes)


# ── Agent — Stateless Transform ───────────────────────────────────────────────
@app.agent(input_topic)
async def transform(stream):
    """Read each sentence, enrich it, and write to the output topic."""
    async for raw_value in stream:
        sentence = raw_value.decode("utf-8")

        enriched = {
            "original": sentence,
            "upper": sentence.upper(),
            "word_count": len(sentence.split()),
            "char_count": len(sentence),
            "processed_at": datetime.now(timezone.utc).isoformat(),
        }

        print(
            f"  [transform] \"{sentence[:55]}\"\n"
            f"              -> words={enriched['word_count']}, "
            f"chars={enriched['char_count']}"
        )

        await output_topic.send(value=json.dumps(enriched).encode("utf-8"))


if __name__ == "__main__":
    app.main()
