"""
Module 13 — Script 04: Stream Output Monitor
=============================================
Course: Apache Kafka (2-Day, Instructor-Led)
Module: 13 — Stream Processing

PURPOSE
-------
Reads from both stream-processing output topics and displays results in
real time. Run this alongside scripts 02 and/or 03 to confirm that the
Faust workers are producing output.

This script uses plain kafka-python (not Faust) to show that stream
processor output is ordinary Kafka messages — any consumer can read them.

HOW TO RUN
----------
  # With scripts 02 and/or 03 already running in other terminals:
  source ../../.venv/bin/activate          # macOS
  ..\\..\\..venv\\Scripts\\Activate.ps1         # Windows PowerShell

  python 04_stream_monitor.py

WHAT TO EXPECT
--------------
  Module 13 — Stream Output Monitor
  Listening on m13-stream-output and m13-wordcount-output ...
  Press Ctrl+C to stop.

  [transform]   "Apache Kafka is a distributed event streaming platform"
                words=8, chars=53, at=2026-05-10T09:14:02

  [word-count]  "apache kafka is a distributed event streaming platform"
                top-5: kafka=3, stream=2, distributed=2, event=2, platform=1

TOPICS USED
-----------
  m13-stream-output      (read)
  m13-wordcount-output   (read)
"""

import json

from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = ["localhost:9092", "localhost:9093", "localhost:9094"]


def _display_transform(value: dict) -> None:
    original = value.get("original", "")
    print(
        f"  [transform]   \"{original[:60]}\"\n"
        f"                words={value.get('word_count')}, "
        f"chars={value.get('char_count')}, "
        f"at={str(value.get('processed_at', ''))[:19]}"
    )


def _display_wordcount(value: dict) -> None:
    sentence = value.get("sentence", "")
    top = value.get("top_words", [])[:5]
    words_str = ", ".join(f"{e['word']}={e['count']}" for e in top)
    print(
        f"  [word-count]  \"{sentence[:55]}\"\n"
        f"                top-5: {words_str}"
    )


def main() -> None:
    consumer = KafkaConsumer(
        "m13-stream-output",
        "m13-wordcount-output",
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="latest",        # only show new messages
        enable_auto_commit=True,
        group_id="m13-monitor-group",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    print("\n  Module 13 — Stream Output Monitor")
    print("  Listening on m13-stream-output and m13-wordcount-output ...")
    print("  Press Ctrl+C to stop.\n")

    try:
        for message in consumer:
            topic = message.topic
            value = message.value

            if topic == "m13-stream-output":
                _display_transform(value)
            elif topic == "m13-wordcount-output":
                _display_wordcount(value)

            print()

    except KeyboardInterrupt:
        print("\n  Stopped by user.")
    finally:
        consumer.close()
        print("  Monitor closed.\n")


if __name__ == "__main__":
    main()
