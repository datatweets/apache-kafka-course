"""
Module 13 — Script 03: Stateful Word Count
===========================================
Course: Apache Kafka (2-Day, Instructor-Led)
Module: 13 — Stream Processing

PURPOSE
-------
Demonstrates STATEFUL stream processing using a Faust Table.

A Faust Table is a distributed key-value store backed by a Kafka changelog
topic. It persists across restarts because the changelog topic is replayed
on startup to rebuild the in-memory state.

This agent:
  1. Reads a sentence from m13-wordcount-input
  2. Splits it into words (filtering common stop words)
  3. Increments each word's count in a persistent Table
  4. Prints the running top-10 words after every sentence
  5. Writes a snapshot of the top words to m13-wordcount-output

KEY CONCEPT — Stateful:
  The output depends on ALL prior input events, not just the current one.
  Processing the 50th sentence uses counts accumulated from the 49 before it.
  If the worker restarts, the Table is rebuilt from its changelog topic and
  counts resume exactly where they left off.

HOW TO RUN
----------
  Terminal 1 — start the producer:
    python 01_stream_producer.py

  Terminal 2 — start this Faust worker (from repository root activate venv):
    source ../../.venv/bin/activate          # macOS
    ..\\..\\..venv\\Scripts\\Activate.ps1         # Windows PowerShell

    python 03_word_count.py worker -l info --web-port 6067

  Terminal 3 — monitor output (optional):
    python 04_stream_monitor.py

  To observe state persistence: stop the worker with Ctrl+C and restart it.
  The word counts resume from the last committed state — the counts do NOT
  reset to zero.

WHAT TO EXPECT
--------------
  [word-count] "apache kafka is a distributed event streaming platform"
  Top words so far:
     1. kafka                4  ████
     2. stream               3  ███
     3. processing           3  ███
     4. events               2  ██
     ...

FAUST CONCEPTS INTRODUCED
--------------------------
  app.Table()    persistent key-value store; backed by a Kafka changelog topic
  default=int    missing keys return 0 (the zero value of int)
  Changelog      Kafka topic that backs the Table; replayed on worker startup
  store="memory://"  in-memory store — fine for learning, no RocksDB needed

TOPICS USED
-----------
  m13-wordcount-input    (read)
  m13-wordcount-output   (write)
"""

import json
from datetime import datetime, timezone

import faust

# ── Faust Application ─────────────────────────────────────────────────────────
app = faust.App(
    "m13-word-count",
    broker="kafka://localhost:9092;localhost:9093;localhost:9094",
    value_serializer="raw",
    store="memory://",   # in-memory; use store="rocksdb://" in production
)

# ── Topic Declarations ────────────────────────────────────────────────────────
input_topic = app.topic("m13-wordcount-input", value_type=bytes)
output_topic = app.topic("m13-wordcount-output", value_type=bytes)

# ── Faust Table — Stateful Word Counts ────────────────────────────────────────
# Faust backs this Table with a Kafka changelog topic named:
#   m13-word-count-m13-wordcount-counts-changelog
# That topic is created automatically by Faust on first startup.
word_counts = app.Table("m13-wordcount-counts", default=int)

# Words that carry no meaning and would dominate the count
STOP_WORDS = {
    "a", "an", "the", "and", "or", "but", "is", "are", "was", "were",
    "be", "been", "being", "have", "has", "had", "do", "does", "did",
    "to", "of", "in", "for", "on", "with", "at", "by", "from", "as",
    "into", "through", "that", "this", "it", "its", "they", "them",
}


def _clean_words(sentence: str) -> list[str]:
    """Lowercase, strip punctuation, and remove stop words."""
    punctuation = str.maketrans("", "", ".,!?;:\"'()-")
    return [
        w for w in sentence.lower().translate(punctuation).split()
        if w and w not in STOP_WORDS
    ]


# ── Agent — Stateful Word Count ───────────────────────────────────────────────
@app.agent(input_topic)
async def count_words(stream):
    """Count words across all sentences using a persistent Faust Table."""
    async for raw_value in stream:
        sentence = raw_value.decode("utf-8")
        words = _clean_words(sentence)

        # Increment counts — state lives in the Faust Table
        for word in words:
            word_counts[word] += 1

        # Build a local sorted snapshot for display and output
        snapshot = sorted(
            ((k, v) for k, v in word_counts.items()),
            key=lambda x: x[1],
            reverse=True,
        )[:10]

        # Display to console
        print(f"\n  [word-count] \"{sentence[:60]}\"")
        print("  Top words so far:")
        for rank, (word, count) in enumerate(snapshot, start=1):
            bar = "█" * min(count, 30)
            print(f"    {rank:>2}. {word:<20} {count:>4}  {bar}")

        # Write snapshot to output topic for script 04 to read
        output = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "sentence": sentence,
            "top_words": [{"word": w, "count": c} for w, c in snapshot],
        }
        await output_topic.send(value=json.dumps(output).encode("utf-8"))


if __name__ == "__main__":
    app.main()
