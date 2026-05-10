# Module 13: Stream Processing with Faust

**Course:** Apache Kafka (2-Day, 14 Hours, Instructor-Led)
**Module duration:** ~75 minutes, hands-on stream processing focused
**Position in course:** Day 2, final module

---

## Learning Objectives

By the end of this module, learners will be able to:

1. Explain the difference between stateless and stateful stream processing
2. Use Faust to build a stream processing agent that reads from a Kafka topic
3. Enrich events in real time with metadata (stateless transformation)
4. Accumulate running state across events using a Faust Table (stateful word count)
5. Verify that stream processor output is standard Kafka messages readable by any consumer
6. Describe how Faust Tables use Kafka changelog topics for fault tolerance

---

## 1. Stream Processing Concepts

Stream processing means transforming or aggregating events **as they arrive**, rather than in scheduled batches.

| Concept | Description |
|---|---|
| Event stream | An unbounded, ordered sequence of messages in a Kafka topic |
| Agent | A Faust async coroutine that consumes a topic stream continuously |
| Stateless processing | Each event is handled independently; no memory of past events |
| Stateful processing | Output depends on accumulated state from prior events |
| Faust Table | Distributed key-value store backed by a Kafka changelog topic |
| Changelog topic | Kafka topic that records every Table mutation; used to rebuild state on restart |
| `store="memory://"` | In-memory state store — simple for learning, not persistent across process restarts |
| `store="rocksdb://"` | RocksDB-backed persistent state store — recommended for production |

### 1.1 Stateless vs Stateful

```
Stateless:  input event ──► transform ──► output event
            (output depends only on the current event)

Stateful:   input event ──► transform + accumulated state ──► output event
            (output depends on current AND all prior events)
```

---

## 2. Topics Used in This Module

| Topic | Direction | Used by |
|---|---|---|
| `m13-stream-input` | Write | Script 01 (producer) |
| `m13-stream-input` | Read | Script 02 (stateless transform) |
| `m13-stream-output` | Write | Script 02 (stateless transform) |
| `m13-stream-output` | Read | Script 04 (monitor) |
| `m13-wordcount-input` | Write | Script 01 (producer) |
| `m13-wordcount-input` | Read | Script 03 (word count) |
| `m13-wordcount-output` | Write | Script 03 (word count) |
| `m13-wordcount-output` | Read | Script 04 (monitor) |

Faust also uses internal topics for coordination and state recovery:

```
m13-stateless-transform-__assignor-__leader
m13-word-count-__assignor-__leader
m13-word-count-m13-wordcount-counts-changelog
```

The course topic creation scripts pre-create these topics because this Kafka 4 lab cluster disables broker-side topic auto-creation. The changelog topic is used to rebuild the word count Table when the worker restarts.

---

## 3. Scripts

### Script 01 — Stream Producer (`01_stream_producer.py`)

**Purpose:** Produces a continuous stream of sentence events to two input topics so that scripts 02 and 03 have data to process.

Uses plain `kafka-python` (no Faust dependency) to write one sentence every 2 seconds, cycling through 15 pre-defined sentences that cover real stream-processing concepts.

**Run:**

```bash
# From repository root — activate virtual environment first
source .venv/bin/activate          # macOS / Linux
.venv\Scripts\Activate.ps1         # Windows PowerShell

python python_scripts/module13/01_stream_producer.py
```

**Expected output:**

```
  Module 13 — Stream Producer
  Producing to m13-stream-input and m13-wordcount-input every 2 seconds
  Press Ctrl+C to stop.

  [   1]  "Apache Kafka is a distributed event streaming platform"
  [   2]  "Stream processing transforms events as they arrive in real time"
```

**Key implementation details:**

- `itertools.cycle(SENTENCES)` loops the 15 sentences indefinitely
- Each message uses a unique key (`sentence-1`, `sentence-2`, …) to distribute across partitions
- The same message is sent to **both** input topics so scripts 02 and 03 can run simultaneously from a single producer

---

### Script 02 — Stateless Transform (`02_stateless_transform.py`)

**Purpose:** Demonstrates stateless stream processing. Each sentence is enriched independently and written to an output topic.

**Run (after starting script 01 in another terminal):**

```bash
python python_scripts/module13/02_stateless_transform.py worker -l info
```

The `worker -l info` arguments are Faust CLI flags — they start the Faust worker process with INFO-level logging.

**Expected output:**

```
  [transform] "Apache Kafka is a distributed event..."
              -> words=8, chars=53
```

**Enrichment fields written to `m13-stream-output`:**

| Field | Description |
|---|---|
| `original` | The raw input sentence |
| `upper` | The sentence in uppercase |
| `word_count` | Number of whitespace-separated words |
| `char_count` | Total character length |
| `processed_at` | UTC ISO-8601 timestamp of processing |

**Key Faust concepts:**

```python
app = faust.App(
    "m13-stateless-transform",          # consumer group prefix
    broker="kafka://...",
    value_serializer="raw",             # receive bytes, decode manually
    store="memory://",
)

input_topic  = app.topic("m13-stream-input",  value_type=bytes)
output_topic = app.topic("m13-stream-output", value_type=bytes)

@app.agent(input_topic)
async def transform(stream):
    async for raw_value in stream:      # async generator — non-blocking loop
        ...
        await output_topic.send(value=json.dumps(enriched).encode())
```

**Why stateless?** The agent processes each sentence independently. Restarting the worker loses nothing — it simply picks up from the last committed offset.

---

### Script 03 — Stateful Word Count (`03_word_count.py`)

**Purpose:** Demonstrates stateful stream processing. A Faust Table accumulates running word counts across all sentences seen so far.

**Run (after starting script 01 in another terminal):**

```bash
python python_scripts/module13/03_word_count.py worker -l info --web-port 6067
```

**Expected output:**

```
  [word-count] "apache kafka is a distributed event streaming platform"
  Top words so far:
     1. kafka                4  ████
     2. stream               3  ███
     3. processing           3  ███
```

**Key Faust concepts:**

```python
# Faust Table — persistent key-value store
word_counts = app.Table("m13-wordcount-counts", default=int)
# missing keys return 0 automatically (default=int → default factory is int())

@app.agent(input_topic)
async def count_words(stream):
    async for raw_value in stream:
        words = _clean_words(raw_value.decode("utf-8"))
        for word in words:
            word_counts[word] += 1      # state mutation recorded in changelog
```

**Stop-word filtering:** Common words (`a`, `the`, `is`, `are`, …) are removed by `_clean_words()` before counting, so the top-words list surfaces meaningful terms.

**State persistence:** Stop the worker (`Ctrl+C`) and restart it. The Table is rebuilt from the changelog topic — word counts resume from where they left off rather than resetting to zero.

**Output to `m13-wordcount-output`** (JSON snapshot after each sentence):

```json
{
  "timestamp": "2026-05-10T09:14:02.123456+00:00",
  "sentence": "Apache Kafka is a distributed event streaming platform",
  "top_words": [
    {"word": "kafka", "count": 4},
    {"word": "stream", "count": 3}
  ]
}
```

---

### Script 04 — Stream Output Monitor (`04_stream_monitor.py`)

**Purpose:** Reads from both output topics simultaneously and displays results as they arrive. Uses plain `kafka-python` (not Faust) to reinforce that stream processor output is ordinary Kafka messages.

**Run (with scripts 02 and/or 03 already running):**

```bash
python python_scripts/module13/04_stream_monitor.py
```

**Expected output:**

```
  Module 13 — Stream Output Monitor
  Listening on m13-stream-output and m13-wordcount-output ...
  Press Ctrl+C to stop.

  [transform]   "Apache Kafka is a distributed event streaming platform"
                words=8, chars=53, at=2026-05-10T09:14:02

  [word-count]  "apache kafka is a distributed event streaming platform"
                top-5: kafka=4, stream=3, processing=3, events=2, platform=1
```

**Key details:**

- `auto_offset_reset="latest"` — only new messages are shown (not replayed history)
- A single `KafkaConsumer` subscribes to both output topics; topic routing is done via `message.topic`

---

## 4. Running the Full Pipeline

Open four terminals from the repository root:

```bash
# Terminal 1 — activate venv then start the producer
source .venv/bin/activate
python python_scripts/module13/01_stream_producer.py

# Terminal 2 — stateless transform worker
source .venv/bin/activate
python python_scripts/module13/02_stateless_transform.py worker -l info

# Terminal 3 — stateful word count worker
source .venv/bin/activate
python python_scripts/module13/03_word_count.py worker -l info --web-port 6067

# Terminal 4 — monitor both output topics
source .venv/bin/activate
python python_scripts/module13/04_stream_monitor.py
```

On Windows PowerShell, replace `source .venv/bin/activate` with `.venv\Scripts\Activate.ps1`.

---

## 5. Classroom Discussion Points

- **Stateless vs Stateful:** Ask learners what happens if you restart script 02 vs script 03. Why do the results differ?
- **Changelog topics:** Run `kafka-topics.sh --list` after starting script 03. Ask learners to identify the auto-created changelog topic.
- **Parallelism:** What happens if you start two instances of the word count worker? (Faust partitions the Table across workers.)
- **Production considerations:** When would you switch from `store="memory://"` to `store="rocksdb://"`?
- **Any consumer can read output:** Script 04 uses kafka-python, not Faust. Why is this significant?

---

## 6. Key Takeaways

- Faust makes it easy to build stream processing pipelines in Python using async generators
- **Stateless agents** transform each event independently — simple and resilient to restarts
- **Stateful agents** use Faust Tables to accumulate state — backed by Kafka changelog topics for fault tolerance
- Stream processor output is just Kafka messages — no special consumer is required to read it
- `store="memory://"` is sufficient for classroom use; switch to `store="rocksdb://"` for production workloads
