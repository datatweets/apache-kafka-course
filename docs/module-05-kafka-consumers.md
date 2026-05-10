# Module 5: Kafka Consumers — Reading Data from Kafka

**Course:** Apache Kafka (2-Day, 14 Hours, Instructor-Led)
**Module duration:** ~90 minutes including hands-on labs
**Position in course:** Day 1, fourth session (after Module 4 — Producers)

---

## Learning Objectives

By the end of this module, learners will be able to:

1. Construct a `KafkaConsumer` in Python with correct deserialisers
2. Use `subscribe()` to join a consumer group and poll for records
3. Explain what a `ConsumerRecord` is and read its fields
4. Describe how Kafka balances partitions across a consumer group
5. Compare auto-commit, manual synchronous commit, and manual async commit
6. Use `assign()` and `seek()` to build a standalone consumer

---

## 1. The Consumer's Role in Kafka

A **consumer** is any application that reads (subscribes to) events from a Kafka topic. Consumers are the downstream processors of the data pipeline.

```
     Broker (partition leader)
               │
     Partition log on disk
     offset: 0  1  2  3  4  5  6 ...
                              ▲
                              │  consumer reads from here
               │
               ▼
┌─────────────────────────────────┐
│          KafkaConsumer          │
│  ┌─────────────────────────┐    │
│  │     Internal Fetch      │    │  ← fetches batches from broker
│  └───────────┬─────────────┘    │
│              │  poll()          │
└──────────────┼──────────────────┘
               │
               ▼
       Your Application
```

The consumer client library (kafka-python in this course) handles:

- Cluster metadata discovery (which broker leads which partition)
- Deserialisation (converting bytes back to Python objects)
- Offset tracking (remembering where in the log the consumer is)
- Consumer group coordination (sharing partitions across instances)
- Heartbeat management (proving the consumer is still alive)
- Fetch batching (retrieving multiple records per network call)

Your application code only calls `poll()`. Everything else is handled by the client library and the broker.

---

## 2. Constructing a KafkaConsumer

### Minimum viable consumer

```python
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    bootstrap_servers=["localhost:9092", "localhost:9093", "localhost:9094"],
    group_id="my-group",
    value_deserializer=lambda b: b.decode("utf-8"),
)
consumer.subscribe(["my-topic"])
```

Only three things are strictly required:

| Argument | Purpose |
|---|---|
| `bootstrap_servers` | Initial broker(s) to contact for cluster metadata |
| `group_id` | Consumer group name — required for offset storage |
| `value_deserializer` | Converts bytes back to a Python object |

### What `bootstrap_servers` actually does

Same as for producers: the consumer connects to one of these addresses, downloads the full cluster metadata, and then talks directly to partition leaders for all subsequent fetch requests.

> **Host vs internal address reminder:**
> Scripts in `python_scripts/` run on the **host machine**, so they use `localhost:9092`.
> Commands inside `docker exec` use `kafka1:29092`. See Module 1, Section 7.

### Deserialisers

Kafka stores raw bytes. The deserialiser is the bridge between Kafka bytes and Python objects.

```python
# bytes → string
value_deserializer=lambda b: b.decode("utf-8")

# bytes → dict (JSON)
value_deserializer=lambda b: json.loads(b.decode("utf-8"))

# Key deserialiser (keys can be None — guard against it)
key_deserializer=lambda b: b.decode("utf-8") if b else None
```

The deserialiser must match the serialiser used by the producer. This contract is called the **schema agreement**.

### Key configuration parameters

```python
consumer = KafkaConsumer(
    bootstrap_servers=["localhost:9092", "localhost:9093", "localhost:9094"],
    group_id="my-group",
    auto_offset_reset="earliest",       # where to start with no history
    enable_auto_commit=True,            # commit offsets automatically
    auto_commit_interval_ms=5000,       # commit every 5 seconds
    key_deserializer=lambda b: b.decode("utf-8") if b else None,
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
)
```

| Parameter | Default | Meaning |
|---|---|---|
| `auto_offset_reset` | `"latest"` | Start from: `"earliest"` = beginning of log, `"latest"` = new messages only |
| `enable_auto_commit` | `True` | Automatically save the consumer's position every `auto_commit_interval_ms` |
| `auto_commit_interval_ms` | `5000` | How often (ms) to commit offsets in the background |
| `max_poll_records` | `500` | Maximum records returned per `poll()` call |
| `session_timeout_ms` | `10000` | Time before broker considers the consumer dead |
| `heartbeat_interval_ms` | `3000` | How often the consumer sends a heartbeat (should be < `session_timeout_ms / 3`) |

---

## 3. The Poll Loop and ConsumerRecord

### The poll loop

```python
consumer.subscribe(["my-topic"])

while True:
    records = consumer.poll(timeout_ms=1000)  # wait up to 1 s for records
    for topic_partition, messages in records.items():
        for msg in messages:
            print(msg.value)
```

`poll()` is the central method of the consumer. Each call:

1. Sends a fetch request to partition leaders
2. Delivers the response as a `{TopicPartition: [ConsumerRecord, ...]}` dict
3. Processes any pending heartbeats and group-management events
4. Returns after `timeout_ms` milliseconds (or sooner if records arrive)

> **Important:** Call `poll()` at least once per `session_timeout_ms` or the broker considers the consumer dead and triggers a rebalance. If your processing per message takes a long time, increase `session_timeout_ms` or reduce `max_poll_records`.

### ConsumerRecord fields

Each `msg` returned by `poll()` is a `ConsumerRecord` named tuple:

| Field | Type | Description |
|---|---|---|
| `msg.topic` | `str` | Topic name |
| `msg.partition` | `int` | Partition number (0-indexed) |
| `msg.offset` | `int` | Position of this record within the partition |
| `msg.key` | `bytes` or `None` | Message key (after deserialisation if configured) |
| `msg.value` | `bytes` or `object` | Message value (after deserialisation if configured) |
| `msg.timestamp` | `int` | Producer timestamp (milliseconds since epoch) |
| `msg.timestamp_type` | `int` | 0 = CreateTime, 1 = LogAppendTime |
| `msg.headers` | `list` | List of `(key, value)` header tuples |
| `msg.checksum` | `int` | CRC32 checksum (deprecated in newer Kafka versions) |

**See:** `python_scripts/module05/01_simple_consumer.py`

---

## 4. Consumer Groups

### What is a consumer group?

A **consumer group** is a set of consumer instances that cooperate to read a topic. Each partition is assigned to exactly one member of the group at any given time. No two members in the same group read the same partition simultaneously.

```
Topic: orders  (3 partitions)

Consumer Group: "order-processor"

  ┌────────────────────────────────────────────┐
  │  Consumer 1 (instance A)                   │
  │  assigned: partition 0                     │
  └────────────────────────────────────────────┘

  ┌────────────────────────────────────────────┐
  │  Consumer 2 (instance B)                   │
  │  assigned: partition 1, partition 2        │
  └────────────────────────────────────────────┘
```

With 3 consumers and 3 partitions, each consumer reads exactly one partition:

```
  Consumer 1 → partition 0
  Consumer 2 → partition 1
  Consumer 3 → partition 2
```

With 4+ consumers and 3 partitions, one consumer will always be idle:

```
  Consumer 1 → partition 0
  Consumer 2 → partition 1
  Consumer 3 → partition 2
  Consumer 4 → (no partition — idle)
```

**Scaling rule:** To increase throughput, add more consumers — up to the number of partitions. Beyond that, extra consumers receive no work.

### The Rebalance Protocol

When consumers join or leave a group, Kafka's **Group Coordinator** broker triggers a rebalance:

1. All consumers stop polling and send a `JoinGroup` request to the coordinator
2. The coordinator elects one consumer as the **group leader**
3. The group leader runs the partition assignment algorithm and reports the result
4. The coordinator distributes the assignment in a `SyncGroup` response
5. Each consumer resumes polling its assigned partitions

During a rebalance, all consumption in the group pauses. Frequent rebalances harm throughput. The main causes are:

- Consumer crashes (session timeout exceeded)
- New consumers joining / existing consumers leaving
- Too-slow processing causing `poll()` to be called infrequently

**See:** `python_scripts/module05/02_consumer_groups.py`

---

## 5. Commits and Offsets

### What is an offset?

An **offset** is a monotonically increasing integer that uniquely identifies a record's position within a single partition log. Partition 0 of a topic has its own independent sequence of offsets starting at 0.

```
Partition 0 log:
offset:  0    1    2    3    4    5    6    7  ...
         │    │    │    │    │    │    │    │
        msg  msg  msg  msg  msg  msg  msg  msg

                              ▲
                              │
                        committed offset = 4
                        (consumer has processed 0..3)
                        (next fetch will start at 4)
```

Kafka stores committed offsets in the internal `__consumer_offsets` topic, keyed by `(group_id, topic, partition)`. On restart, the consumer fetches this value and resumes from there.

### Auto-commit (default)

```python
consumer = KafkaConsumer(
    ...,
    enable_auto_commit=True,
    auto_commit_interval_ms=5000,  # commit every 5 seconds
)
```

Kafka's background thread commits the consumer's current position every `auto_commit_interval_ms` milliseconds. `close()` always does a final synchronous commit on shutdown.

**Delivery semantics:** at-least-once. If the process crashes mid-poll (after receiving records, before the next auto-commit fires), those records will be re-delivered on restart.

**See:** `python_scripts/module05/03_commits_and_offsets.py --mode auto_commit`

### Manual synchronous commit

```python
consumer = KafkaConsumer(..., enable_auto_commit=False)
consumer.subscribe(["my-topic"])

while True:
    records = consumer.poll(timeout_ms=1000)
    process(records)           # 1. process the batch
    consumer.commit()          # 2. commit AFTER successful processing
```

`consumer.commit()` blocks until the broker confirms the commit. This ensures the offset is durably stored before moving to the next batch.

**Delivery semantics:**
- At-least-once if you commit after processing (crash between step 1 and 2 = re-delivery)
- At-most-once if you commit before processing (crash after commit, before processing = data loss)

**See:** `python_scripts/module05/03_commits_and_offsets.py --mode manual_sync`

### Manual asynchronous commit

```python
def on_commit(offsets, exception):
    if exception:
        print(f"Commit failed: {exception}")

consumer = KafkaConsumer(..., enable_auto_commit=False)

try:
    while True:
        records = consumer.poll(timeout_ms=1000)
        process(records)
        consumer.commit_async(callback=on_commit)  # non-blocking
finally:
    consumer.commit()   # final synchronous commit on clean shutdown
    consumer.close()
```

`commit_async()` fires the commit request in the background and returns immediately. The poll loop continues without waiting for broker confirmation. Throughput is higher because the network round-trip to the broker coordinator never blocks polling.

**Best practice:** Always pair `commit_async()` with a final `consumer.commit()` in a `finally` block to protect the last batch on clean shutdown.

**See:** `python_scripts/module05/03_commits_and_offsets.py --mode manual_async`

### Seeking (replaying records)

```python
consumer.subscribe(["my-topic"])
consumer.poll(timeout_ms=3000)  # first poll triggers partition assignment

assigned = consumer.assignment()
consumer.seek_to_beginning(*assigned)  # move fetch cursor to offset 0
```

`consumer.seek_to_beginning(*partitions)` resets the fetch position to the earliest available offset. The next `poll()` call starts re-reading from the beginning. Offsets are not committed, so seeking does not affect what other consumer groups (or restarts of this group) will read.

**Use cases:** Replaying events after a bug fix, populating a new downstream system from the full topic history, testing.

**See:** `python_scripts/module05/03_commits_and_offsets.py --mode seek_to_start`

### Commit strategy comparison

| Strategy | Blocks poll loop | Throughput | Delivery guarantee |
|---|---|---|---|
| Auto-commit | No (background timer) | Highest | At-least-once |
| Manual synchronous | Yes (per batch) | Lower | At-least-once or at-most-once |
| Manual async | No (fire and forget) | High | At-least-once (with final sync commit) |

---

## 6. Standalone Consumer (assign vs subscribe)

### Two consumer modes

Kafka consumers have two fundamentally different modes:

| Mode | Method | Group protocol | Rebalancing |
|---|---|---|---|
| Consumer group | `subscribe(topics)` | Yes — JoinGroup, SyncGroup, Heartbeat | Automatic |
| Standalone | `assign(topic_partitions)` | No | Never — you manage the assignment |

### TopicPartition

```python
from kafka import TopicPartition

# Identify one partition of one topic
tp = TopicPartition("orders", 0)  # topic="orders", partition=0
```

`TopicPartition` is a named tuple used throughout the consumer API: `assign()`, `seek()`, `beginning_offsets()`, `end_offsets()`, `committed()`, `position()`.

### assign()

```python
from kafka import KafkaConsumer, TopicPartition

consumer = KafkaConsumer(
    bootstrap_servers=[...],
    group_id=None,            # no group protocol
    enable_auto_commit=False,
)

tps = [TopicPartition("orders", 0), TopicPartition("orders", 1)]
consumer.assign(tps)          # takes effect immediately — no poll() needed
```

With `group_id=None`:
- No `JoinGroup` / `SyncGroup` requests are sent to the broker
- No offsets are committed to `__consumer_offsets`
- No rebalancing ever occurs
- The consumer can still query broker offsets and seek freely

### Querying offset boundaries

```python
beginning = consumer.beginning_offsets(tps)   # earliest readable offset per tp
end = consumer.end_offsets(tps)               # next-to-be-written offset per tp

for tp in tps:
    available = end[tp] - beginning[tp]
    print(f"partition {tp.partition}: {available} records available")
```

`beginning_offsets()` returns the earliest offset (usually 0, but may be higher if old log segments have been deleted by retention policy).

`end_offsets()` returns the **next** offset to be written — the offset of the latest record is `end_offsets[tp] - 1`.

### seek() and seek_to_beginning()

```python
# Seek a single partition to a specific offset
consumer.seek(tp, 42)

# Seek multiple partitions to their earliest available offsets
consumer.seek_to_beginning(*tps)

# Seek multiple partitions to their latest offset (useful for tail-following)
consumer.seek_to_end(*tps)
```

`seek()` takes effect immediately after the call. The very next `poll()` fetches from that position.

**See:** `python_scripts/module05/04_standalone_consumer.py`

### When to use a standalone consumer

| Use case | Reason |
|---|---|
| Forensic inspection | Read specific offsets without affecting a group's committed position |
| Bounded replay | Know exactly how many records exist (`end - beginning`) and stop when done |
| Integration tests | Deterministic reads without interference from other group members |
| Exactly-once pipelines | Store offsets in the same database transaction as the processed results |
| Admin tooling | Inspect log boundaries, check lag, compare beginning vs. end offsets |

---

## 7. Hands-on Lab

### Prerequisites

Ensure your Docker environment is running:

```bash
cd docker
docker compose up -d
docker compose ps   # all services should show "healthy"
```

Activate the Python virtual environment:

```bash
cd python_scripts/module05

# macOS / Linux
source ../../.venv/bin/activate

# Windows PowerShell
..\..\\.venv\Scripts\Activate.ps1
```

### Lab 1 — Simple Consumer

```bash
python 01_simple_consumer.py
```

Expected observations:
- 12 seed messages produced to `m5-consumer-topic`
- Consumer joins group `m5-simple-group`, receives partition assignment
- Each `ConsumerRecord` field (topic, partition, offset, key, value) printed
- Consumer stops after receiving all 12 messages or 5 empty polls

### Lab 2 — Consumer Groups

```bash
# Two consumers share 3 partitions (2 + 1 assignment)
python 02_consumer_groups.py

# Three consumers — one partition each
python 02_consumer_groups.py --consumers 3

# Four consumers — one will be idle
python 02_consumer_groups.py --consumers 4
```

Expected observations:
- With 2 consumers: one receives 2 partitions, the other receives 1
- With 3 consumers: each receives exactly 1 partition
- With 4 consumers: one consumer shows `partitions_assigned=0`
- Total messages across all consumers equals 30 for a fresh group; if the same group has uncommitted history from prior runs the total may be higher

### Lab 3 — Commits and Offsets

```bash
# Automatic background commit (simplest, default)
python 03_commits_and_offsets.py --mode auto_commit

# Manual synchronous commit after each batch
python 03_commits_and_offsets.py --mode manual_sync

# Manual async commit (non-blocking, callback on completion)
python 03_commits_and_offsets.py --mode manual_async

# Seek to beginning and re-read all messages
python 03_commits_and_offsets.py --mode seek_to_start --no-seed
```

Expected observations:
- `auto_commit`: messages show "(auto)" commit label; offsets saved in background
- `manual_sync`: each batch shows "batch N committed" after blocking commit
- `manual_async`: COMMITTED lines printed from the background callback thread
- `seek_to_start`: all messages re-read from offset 0 regardless of prior commits

### Lab 4 — Standalone Consumer

```bash
# Read all 3 partitions from the beginning (no group protocol)
python 04_standalone_consumer.py

# Read only partitions 0 and 1
python 04_standalone_consumer.py --partitions 0 1

# Start reading from offset 3 on each partition
python 04_standalone_consumer.py --from-offset 3 --no-seed

# Read only the first 5 messages
python 04_standalone_consumer.py --max-messages 5 --no-seed
```

Expected observations:
- Partition log state table shows `beginning`, `end`, and `available` counts per partition
- No group ID printed (standalone — no group protocol)
- With `--from-offset N`: reading starts at offset N on each partition
- Offsets are never committed (`__consumer_offsets` not updated)

---

## 8. Module 5 Summary

| Concept | Key point |
|---|---|
| `bootstrap_servers` | Entry point for metadata discovery — not the full broker list |
| `value_deserializer` | Kafka stores bytes; the deserialiser converts bytes back to Python objects |
| `subscribe()` | Join a consumer group — Kafka manages partition assignment |
| `poll()` | Fetch records, send heartbeats, process group-management events |
| `ConsumerRecord` | Named tuple: topic, partition, offset, key, value, timestamp, headers |
| Consumer group | Multiple instances share partitions — max parallelism = partition count |
| Rebalance | Triggered on join/leave — all consumption pauses until complete |
| Offset | Monotonically increasing integer identifying a record's position in a partition |
| `__consumer_offsets` | Internal Kafka topic storing committed offsets per `(group, topic, partition)` |
| `auto_offset_reset` | Where to start when no committed offset exists: `earliest` or `latest` |
| `enable_auto_commit` | Background timer commits; `close()` does a final synchronous commit |
| `commit()` | Manual synchronous commit — blocks until broker confirms |
| `commit_async()` | Manual async commit — non-blocking, optional callback |
| `assign()` | Standalone mode — no group protocol, no rebalancing |
| `TopicPartition` | Named tuple `(topic, partition)` used throughout the consumer API |
| `seek()` | Move fetch cursor to a specific offset; takes effect on next `poll()` |
| `beginning_offsets()` | Query the earliest readable offset per partition |
| `end_offsets()` | Query the next-to-be-written offset per partition |

---

## Module 5 Exit Criteria

A learner is ready for Module 6 when:

- [ ] `01_simple_consumer.py` runs successfully and prints all `ConsumerRecord` fields
- [ ] Learner can explain what a `ConsumerRecord` is and describe its key fields
- [ ] `02_consumer_groups.py --consumers 3` shows each consumer assigned exactly one partition
- [ ] Learner can explain why a 4th consumer would be idle on a 3-partition topic
- [ ] `03_commits_and_offsets.py --mode manual_sync` shows batch commit lines after each batch
- [ ] Learner can explain the at-least-once vs at-most-once commit trade-off
- [ ] `04_standalone_consumer.py --partitions 0 --no-seed` shows the partition log state table and reads from offset 0
- [ ] Learner can explain the difference between `assign()` and `subscribe()`

---

## Further Reading

- [kafka-python KafkaConsumer docs](https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html)
- [Kafka documentation — Consumer Configs](https://kafka.apache.org/documentation/#consumerconfigs)
- [Kafka documentation — The Log: What every software engineer should know about real-time data's unifying abstraction](https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying)
- [KIP-62 — Allow consumer to send heartbeats from a background thread](https://cwiki.apache.org/confluence/display/KAFKA/KIP-62%3A+Allow+consumer+to+send+heartbeats+from+a+background+thread)
