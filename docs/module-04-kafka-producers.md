# Module 4: Kafka Producers — Writing Messages to Kafka

**Course:** Apache Kafka (2-Day, 14 Hours, Instructor-Led)
**Module duration:** ~90 minutes including hands-on labs
**Position in course:** Day 1, third session (after Module 3 — KRaft internals)

---

## Learning Objectives

By the end of this module, learners will be able to:

1. Construct a `KafkaProducer` in Python with correct serialisers
2. Send a message using fire-and-forget, synchronous, and async-callback patterns
3. Explain what a message key is and how it controls partition routing
4. Tune producer configuration for either throughput or durability
5. Describe the idempotent producer and when to use it

---

## 1. The Producer's Role in Kafka

A **producer** is any application that writes (publishes) events to a Kafka topic. The producer is the entry point for all data into the Kafka cluster.

```
Your Application
      │
      │  producer.send("orders", key="C001", value={...})
      ▼
┌─────────────────────────────────┐
│          KafkaProducer          │
│  ┌─────────────────────────┐    │
│  │  Internal Record Buffer │    │  ← records accumulate here
│  └───────────┬─────────────┘    │
│              │  batch flush     │
└──────────────┼──────────────────┘
               │
               ▼
     Broker (partition leader)
               │
               ▼
     Partition log on disk
               │
               ├──► Replica on kafka2
               └──► Replica on kafka3
```

The producer client library (kafka-python in this course) handles:

- Cluster metadata discovery (which broker leads which partition)
- Serialisation (converting Python objects to bytes)
- Batching (accumulating records for efficiency)
- Compression
- Retry on transient failures
- Acknowledgement tracking

Your application code only calls `send()`. Everything else is handled by the client library and the broker.

---

## 2. Constructing a KafkaProducer

### Minimum viable producer

```python
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092", "localhost:9093", "localhost:9094"],
    value_serializer=lambda message: message.encode("utf-8"),
)
```

Only two arguments are strictly required:

| Argument | Purpose |
|---|---|
| `bootstrap_servers` | Initial broker(s) to contact for cluster metadata |
| `value_serializer` | Converts the Python value to bytes before the network send |

### What `bootstrap_servers` actually does

`bootstrap_servers` is **not** the full broker list used for every request. The producer connects to one of these addresses, downloads the full cluster metadata (which broker leads which partition), and then talks directly to partition leaders for every subsequent produce request.

You only need to list enough brokers that at least one is reachable. Listing all three, as done in this course, makes the initial connection more resilient.

> **Host vs internal address reminder:**
> Scripts in `python_scripts/` run on the **host machine**, so they use `localhost:9092`.
> Commands inside `docker exec` use `kafka1:29092`. See Module 1, Section 7.

### Serialisers — why Kafka stores bytes

Kafka is transport-agnostic. It stores and transfers raw bytes on disk and over the network. It has no knowledge of JSON, Avro, Protobuf, or strings. Your serialiser is the bridge between Python objects and Kafka bytes.

```python
# String → bytes
value_serializer=lambda msg: msg.encode("utf-8")

# Dict → JSON bytes
value_serializer=lambda msg: json.dumps(msg).encode("utf-8")

# Key serialiser — same concept, applied to the message key
key_serializer=lambda key: key.encode("utf-8")
```

The consumer must use the **matching deserialiser** to reconstruct the original object. This contract between producer and consumer is called the **schema agreement**. In production systems this is enforced by a Schema Registry (Confluent Schema Registry with Avro is common). That topic is out of scope for this module.

---

## 3. Sending a Message

### Pattern 1 — Fire-and-forget

```python
producer.send(TOPIC, value="Hello, Kafka!")
producer.flush()
```

`send()` is **non-blocking**. It places the record in an internal buffer and returns a `FutureRecordMetadata`. The actual network I/O happens on a background thread. Fire-and-forget ignores the Future entirely.

**When to use:** Maximum throughput, where losing an occasional message during a crash is acceptable (e.g. metrics, logging).

**See:** `python_scripts/module04/01_simple_producer.py`

---

### Pattern 2 — Synchronous send (with confirmation)

```python
record_metadata = producer.send(TOPIC, key=key, value=event).get(timeout=10)
print(f"partition={record_metadata.partition}  offset={record_metadata.offset}")
```

`.get(timeout=10)` blocks the calling thread until the broker acknowledges the record (or raises `KafkaError` after 10 seconds). You get back a `RecordMetadata` object:

| Attribute | Value |
|---|---|
| `record_metadata.topic` | Name of the topic |
| `record_metadata.partition` | Partition the record was written to |
| `record_metadata.offset` | Offset assigned to this record within the partition |

**When to use:** Teaching, debugging, and scenarios where you need confirmation per message before sending the next (e.g. ordered transactional workflows).

**Trade-off:** Each `.get()` waits for a broker round-trip, so throughput is limited to `1 / round-trip-latency` messages per second per thread.

**See:** `python_scripts/module04/02_keyed_producer.py`

---

### Pattern 3 — Asynchronous send with callbacks

```python
(
    producer.send(TOPIC, key=key, value=payload)
    .add_callback(on_send_success)
    .add_errback(on_send_error)
)
```

The producer sends records without waiting. When each record is acknowledged, the client library invokes your callback on its I/O thread.

```python
def on_send_success(record_metadata):
    # Called when the broker confirms the write
    pass

def on_send_error(exc):
    # Called when delivery failed after all retries
    pass
```

**When to use:** Production bulk-send, pipeline ingestion, any high-throughput scenario.

**Important:** Callbacks run on the producer's **background I/O thread**, not your main thread. If you update shared state inside a callback, protect it with a `threading.Lock`.

**See:** `python_scripts/module04/03_async_producer_callbacks.py`

---

### Comparison of sending patterns

| Pattern | Blocks main thread | Throughput | Delivery confirmation |
|---|---|---|---|
| Fire-and-forget | No | Highest | None in code |
| Synchronous `.get()` | Yes (per message) | Lowest | Yes, inline |
| Async + callback | No | High | Yes, via callback |

---

## 4. Message Keys and Partition Routing

### What is a key?

A Kafka record has an optional **key**. The key is separate from the value. It is also serialised to bytes and sent alongside the value.

```python
producer.send(
    topic="orders",
    key="C001",          # customer ID as the routing key
    value={"order_id": "ORD-100", "amount": 49.99},
)
```

If no key is specified (or `key=None`), Kafka distributes records across partitions in a round-robin fashion.

### How the default partitioner works

```
partition = murmur2_hash(key_bytes) % num_partitions
```

The MurmurHash2 algorithm is deterministic: the same key bytes always produce the same hash, and therefore the same partition number. This is guaranteed across all Kafka client libraries and broker versions.

```
key="C001"  →  hash → 7283920  →  7283920 % 3 = 1  →  partition 1
key="C002"  →  hash → 3821042  →  3821042 % 3 = 2  →  partition 2
key="C003"  →  hash → 9102831  →  9102831 % 3 = 0  →  partition 0
```

Every future event for `C001` will always go to partition 1, regardless of when it is sent or which producer sends it (as long as the topic partition count does not change).

### Why keys matter

**Ordering guarantee:** Within a partition, records are stored and delivered in the order they were written. If all events for a single entity (customer, device, order) share the same key, a consumer processing that partition receives a complete, ordered history of that entity.

**Consumer scalability:** Consumer group members each process a subset of partitions. Because all events for `C001` are in one partition, one consumer processes all `C001` history — with no cross-consumer coordination needed.

**Compaction:** Topics configured with `cleanup.policy=compact` retain only the latest record per key. This is how Kafka implements a key-value store on top of the log.

> **Warning:** Changing the number of partitions on an existing topic breaks key-to-partition determinism. Existing records stay where they are; new records for the same key may go to a different partition. Avoid repartitioning in production unless you are prepared to handle this transition.

**See:** `python_scripts/module04/02_keyed_producer.py`

---

## 5. Configuring the Producer

Producer configuration falls into four categories: **durability**, **batching**, **compression**, and **flow control**.

### 5.1 Durability — `acks`

`acks` controls how many broker acknowledgements the producer requires before considering a send successful.

| Value | Meaning | Durability | Latency |
|---|---|---|---|
| `0` | Do not wait for any acknowledgement | Lowest — fire-and-forget at network level | Lowest |
| `1` | Leader broker has written to its log | Medium — lost if leader crashes before replication | Medium |
| `"all"` | All in-sync replicas (ISR) have written | Highest — survives leader failure | Highest |

```python
# Maximum durability — no data loss even on broker failure
producer = KafkaProducer(
    bootstrap_servers=[...],
    acks="all",
    ...
)
```

`acks="all"` works together with the broker setting `min.insync.replicas` (default 1, recommended 2 for a 3-broker cluster). If fewer than `min.insync.replicas` brokers are in-sync, the broker refuses the write and the producer raises `NotEnoughReplicasException`.

### 5.2 Retries and ordering

```python
producer = KafkaProducer(
    bootstrap_servers=[...],
    retries=5,                                    # retry up to 5 times
    retry_backoff_ms=300,                         # 300 ms between retries
    max_in_flight_requests_per_connection=1,      # preserve ordering
)
```

**The ordering problem with retries:**

If `max_in_flight_requests_per_connection > 1` and a retry occurs, a later batch can be acknowledged before an earlier retried batch, resulting in out-of-order records. Setting it to `1` prevents this — at the cost of throughput.

The clean solution is the **idempotent producer** (see Section 5.4), which allows `max_in_flight_requests_per_connection=5` without the ordering risk.

### 5.3 Batching and compression

```python
producer = KafkaProducer(
    bootstrap_servers=[...],
    linger_ms=20,               # wait up to 20 ms to fill the batch
    batch_size=32768,           # max 32 KB per partition batch
    compression_type="gzip",   # compress the batch before sending
)
```

**`linger_ms`:** The producer waits up to this many milliseconds before sending a batch, even if `batch_size` is not reached. This is the primary knob for trading latency for throughput. Default is 0 (send immediately).

**`batch_size`:** Maximum bytes accumulated per partition before forcing a send. Default is 16 384 bytes (16 KB). When the batch is full, it is sent regardless of `linger_ms`.

**Compression options:**

| Option | Speed | Ratio | Best for |
|---|---|---|---|
| `None` (default) | Fastest | None | Low-latency, small messages |
| `"gzip"` | Slow | Best | Archival, low-bandwidth links |
| `"snappy"` | Fast | Good | General production use |
| `"lz4"` | Very fast | Good | High-throughput pipelines |
| `"zstd"` | Fast | Best-in-class | Modern production use |

Compression happens on the producer, not the broker. Compressed batches are stored and replicated as-is, and decompressed by the consumer.

### 5.4 Idempotent producer

```python
producer = KafkaProducer(
    bootstrap_servers=[...],
    enable_idempotence=True,   # automatically sets acks="all", retries>0
)
```

With `enable_idempotence=True`:

- The broker assigns this producer a **Producer ID (PID)**
- Every record sent to a partition carries a **sequence number**
- If the producer retries a failed send, the broker uses the PID + sequence number to detect and discard the duplicate
- The record is written **exactly once** per producer session

**Limits of idempotence:**

- Exactly-once is guaranteed within one producer session (one process lifetime)
- If the producer process restarts, a new PID is assigned — the broker cannot deduplicate across sessions
- For cross-session exactly-once, use Kafka Transactions (Module 7)

### 5.5 Flow control — buffer and blocking

```python
producer = KafkaProducer(
    bootstrap_servers=[...],
    buffer_memory=33554432,    # 32 MB total buffer (default)
    max_block_ms=60000,        # block send() for up to 60 s if buffer full
)
```

**`buffer_memory`:** Total bytes the producer can accumulate across all partitions before `send()` starts blocking. If your producer outpaces the broker, the buffer fills.

**`max_block_ms`:** How long `send()` will block waiting for buffer space before raising `BufferError`. Default 60 seconds.

In practice, if you see `BufferError` in production, the broker is slower than the producer — check broker health, network, or reduce `linger_ms` to flush more frequently.

### Configuration profiles summary

| Profile | `acks` | `linger_ms` | `max_in_flight` | `compression` | `enable_idempotence` |
|---|---|---|---|---|---|
| Throughput | `1` | 20 | 5 | `lz4` | False |
| Reliable | `"all"` | 5 | 1 | `gzip` | False |
| Idempotent | `"all"` | 10 | 1 | — | True |
| Low-latency | `1` | 0 | 1 | None | False |

**See:** `python_scripts/module04/04_producer_configuration.py`

---

## 6. Hands-On Lab

### Prerequisites

- Docker platform is running (`docker compose up -d` from `docker/`)
- Topics created (`bash scripts/create-topics.sh`)
- Virtual environment activated

### Step 1 — Simple fire-and-forget

```bash
cd python_scripts/module04
python 01_simple_producer.py
```

Open Kafdrop at `http://localhost:9000` → `m4-simple-topic` → Messages. You should see one message on one partition.

### Step 2 — Keyed partition routing

```bash
python 02_keyed_producer.py
```

Observe the terminal output — each line shows the partition and offset assigned to that event. Confirm that all `C001` events share the same partition number, as do all `C002` and `C003` events.

Then in Kafdrop, browse `m4-keyed-topic` and inspect each partition's messages. Each partition holds exactly one customer's event history.

### Step 3 — Async producer with callbacks

```bash
python 03_async_producer_callbacks.py
```

The script sends 100 sensor readings without blocking on each acknowledgement. It prints a throughput report at the end. Note the messages-per-second rate — it will be significantly higher than the synchronous `.get()` approach in Script 02.

### Step 4 — Configuration profiles comparison

Run all four profiles and record the average latency for each:

```bash
python 04_producer_configuration.py --profile latency
python 04_producer_configuration.py --profile throughput
python 04_producer_configuration.py --profile reliable
python 04_producer_configuration.py --profile idempotent
```

Expected observations:
- `latency` profile: lowest avg latency, lowest throughput (`batch_size=1`, `linger_ms=0`)
- `throughput` profile: highest messages/s (`linger_ms=20`, `acks=1`)
- `reliable` profile: highest per-message latency (`acks="all"`, `max_in_flight=1`)
- `idempotent` profile: similar latency to reliable, but uses `max_in_flight=1` (kafka-python requires this with `enable_idempotence=True`)

---

## 7. Module 4 Summary

| Concept | Key point |
|---|---|
| `bootstrap_servers` | Entry point for metadata discovery — not the full broker list |
| `value_serializer` | Kafka stores bytes; the serialiser converts Python objects to bytes |
| Fire-and-forget | Non-blocking, highest throughput, no delivery confirmation |
| Synchronous `.get()` | Blocks per message, confirms partition and offset |
| Async + callback | Non-blocking, high throughput, delivery notification via callback |
| Message key | Determines partition via `murmur2_hash(key) % num_partitions` |
| Key guarantee | Same key → same partition → ordered delivery for that entity |
| `acks="all"` | All in-sync replicas must confirm — highest durability |
| `linger_ms` | Batching wait — increase for throughput, decrease for latency |
| `enable_idempotence` | Exactly-once per producer session via PID + sequence number |

---

## Module 4 Exit Criteria

A learner is ready for Module 5 when:

- [ ] `01_simple_producer.py` runs successfully and the message appears in Kafdrop
- [ ] `02_keyed_producer.py` output shows all events for each customer in the same partition
- [ ] `03_async_producer_callbacks.py` delivers 100 messages with 0 errors
- [ ] Learner can explain the difference between `acks=1` and `acks="all"` in their own words
- [ ] Learner can explain why `max_in_flight_requests_per_connection=1` is needed with retries when `enable_idempotence=False`
- [ ] `04_producer_configuration.py --profile idempotent` runs without errors

---

## Further Reading

- [kafka-python KafkaProducer docs](https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html)
- [Kafka documentation — Producer Configs](https://kafka.apache.org/documentation/#producerconfigs)
- [KIP-98 — Exactly Once Delivery and Transactional Messaging](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging)
