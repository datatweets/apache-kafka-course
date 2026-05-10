# Module 9: Building a Real-Time Data Pipeline with MySQL, Kafka, and Elasticsearch

## Learning Objectives

By the end of this module you will be able to:

1. Describe the role of each component in a MySQL → Kafka → Elasticsearch pipeline
2. Explain the high-water mark polling pattern and its trade-offs versus CDC
3. Construct a KafkaProducer that wraps database rows in a metadata envelope
4. Construct a KafkaConsumer that sinks events into Elasticsearch idempotently
5. Use consumer groups to allow multiple sink instances to share the load
6. Observe end-to-end pipeline latency using the monitor script

---

## 1. Pipeline Architecture

```
┌──────────┐  Poll (id > last_seen)  ┌────────────┐  KafkaProducer  ┌─────────────────────┐
│  MySQL   │ ──────────────────────► │  Script 02 │ ──────────────► │ Kafka Cluster (×3)  │
│ 3307     │                         │  producer  │                 │                     │
└──────────┘                         └────────────┘                 │ m9-mysql-customers  │
                                                                    │ m9-mysql-orders     │
┌─────────────────┐  PUT /_doc/{id}  ┌────────────┐  KafkaConsumer  │                     │
│ Elasticsearch   │ ◄─────────────── │  Script 03 │ ◄────────────── │                     │
│ 9200            │                  │  consumer  │                 └─────────────────────┘
│ m9-customers    │                  └────────────┘
│ m9-orders       │
└─────────────────┘
```

All three components run simultaneously in separate terminals.
Script 01 (explorer) and Script 04 (monitor) are observation tools you run on demand.

---

## 2. Source System: MySQL

The `kafka_course` database contains two tables:

| Table | Key columns | Description |
|-------|-------------|-------------|
| `customers` | id, name, email, country, created_at, updated_at | Customer master data |
| `orders` | id, customer_id, product, amount, status, created_at, updated_at | Order transactions |

Both tables have an auto-incremented `id` column.  Script 02 uses this column
as the **high-water mark**: it remembers the highest `id` it has published and
on each poll only fetches rows with a larger `id`.

### High-Water Mark vs. Change Data Capture (CDC)

| Approach | What it captures | Misses | Typical tool |
|----------|-----------------|--------|--------------|
| High-water mark (Script 02) | New INSERT rows | UPDATE, DELETE | Custom code |
| CDC / Debezium | INSERT, UPDATE, DELETE, schema changes | Nothing | Kafka Connect + Debezium |

Module 10 will replace Script 02 with a Kafka Connect Debezium connector that
captures full CDC events automatically.

---

## 3. Script 01 — Explore the MySQL Source

**File:** `python_scripts/module09/01_explore_mysql_source.py`

Run this first to familiarise yourself with the schema and seed data before
building the pipeline.

```bash
# Run from the repository root
source .venv/bin/activate
python python_scripts/module09/01_explore_mysql_source.py
```

**Expected output:**

```
============================================================
  Module 9 — MySQL Source Explorer
============================================================

--- Table: customers ---
  Schema (DESCRIBE customers):
    id              | int            | NO  | PRI |
    name            | varchar(255)   | YES |     |
    ...

  Row count: 5

  Sample rows:
    {'id': 1, 'name': 'Alice Tan', 'email': 'alice@example.com', ...}
    ...
```

### What to look for

- Verify MySQL connectivity (port 3307 → container port 3306)
- Note the `id` data type — int, auto-incremented — this becomes the Kafka key
- Note `updated_at` — useful for ordering if you add UPDATE tracking later

---

## 4. Script 02 — MySQL → Kafka Producer

**File:** `python_scripts/module09/02_mysql_to_kafka_producer.py`

This script is the **source connector** of the pipeline.

### Key design decisions

#### 4.1 JSON Envelope

Every row is wrapped in an envelope before being published:

```json
{
  "source": "mysql",
  "table": "customers",
  "operation": "snapshot_or_insert",
  "emitted_at": "2025-07-01T10:00:00.123456+00:00",
  "data": {
    "id": 6,
    "name": "Fatima Al-Amin",
    "email": "fatima.al-amin@example.com",
    "country": "Morocco",
    "created_at": "2025-07-01T10:00:00",
    "updated_at": "2025-07-01T10:00:00"
  }
}
```

The envelope makes the event self-describing.  A consumer reading the Kafka topic
does not need to know which database or table the data came from — it's all in the
envelope.

#### 4.2 Message Key

The Kafka message key is set to the MySQL row `id` (as a UTF-8 string).  This means:

- All events for the same row always go to the **same partition** (hash routing)
- Consumers that need to process a row's history in order receive events in insertion order
- The key becomes the Elasticsearch document id, enabling idempotent updates

#### 4.3 Producer Configuration

| Setting | Value | Reason |
|---------|-------|--------|
| `acks` | `"all"` | Wait for all in-sync replicas — no data loss |
| `retries` | `5` | Retry transient failures |
| `key_serializer` | UTF-8 string → bytes | Readable in Kafdrop |
| `value_serializer` | `json.dumps` → bytes | JSON is the common data exchange format |

### Run

```bash
# Terminal 1 — continuous polling every 5 seconds:
python python_scripts/module09/02_mysql_to_kafka_producer.py

# Terminal 1 — one-shot (reads everything currently in MySQL and exits):
python python_scripts/module09/02_mysql_to_kafka_producer.py --once
```

### Expected output

```
============================================================
  Module 9 — MySQL → Kafka Producer
  Mode: continuous (every 5s)
============================================================

[10:01:15] Polling MySQL ...
  [customers] id=1  →  topic=m9-mysql-customers
  [customers] id=2  →  topic=m9-mysql-customers
  ...
  Flushed 5 record(s) from 'customers' to 'm9-mysql-customers'
  [orders] id=1  →  topic=m9-mysql-orders
  ...
  Flushed 5 record(s) from 'orders' to 'm9-mysql-orders'
  Sleeping 5s before next poll ...
```

### Verify with Kafka console consumer

```bash
# In a new terminal — read from the beginning of the customers topic:
docker exec -it kafka1 kafka-console-consumer.sh \
  --bootstrap-server kafka1:29092 \
  --topic m9-mysql-customers \
  --from-beginning \
  --max-messages 3
```

---

## 5. Script 03 — Kafka → Elasticsearch Consumer

**File:** `python_scripts/module09/03_kafka_to_elasticsearch_consumer.py`

This script is the **sink connector** of the pipeline.

### Key design decisions

#### 5.1 Consumer Group

```python
group_id="m9-es-sink"
```

A **consumer group** is a logical label.  Multiple instances of Script 03 with the
same `group_id` cooperate: Kafka distributes the topic partitions across them so
no partition is read twice.

If you run a second instance, Kafka will **rebalance** — each instance takes half
the partitions.  Stop one instance and Kafka rebalances again, giving all partitions
back to the remaining instance.

#### 5.2 `auto_offset_reset="earliest"`

When the consumer group `m9-es-sink` has no committed offset (first run, or after
a reset), reading starts from partition offset 0 — the very first message ever
written.  This guarantees Script 03 processes all historical events even if it
starts after Script 02.

Change to `"latest"` if you want to skip historical messages and only process
messages produced after the consumer starts.

#### 5.3 Idempotent Elasticsearch Indexing

```
PUT /m9-customers/_doc/{row_id}
```

Using `PUT` with an explicit document id makes indexing idempotent:

- If the document does not exist → Elasticsearch creates it
- If the document already exists → Elasticsearch updates it in place
- Running Script 03 twice produces the same result — no duplicates

This is critical because Kafka guarantees **at-least-once** delivery by default.
The same Kafka message can be delivered more than once (e.g. after a consumer
restart).  Idempotent indexing absorbs redelivery safely.

### Run

```bash
# Terminal 2 — run until interrupted:
python python_scripts/module09/03_kafka_to_elasticsearch_consumer.py

# Terminal 2 — process first 10 messages then exit:
python python_scripts/module09/03_kafka_to_elasticsearch_consumer.py --max-messages 10
```

### Expected output

```
============================================================
  Module 9 — Kafka → Elasticsearch Consumer
  Reading: unlimited messages | group: m9-es-sink
============================================================

  Created index: m9-customers
  Created index: m9-orders

Waiting for messages ... (Ctrl+C to stop)

  [m9-mysql-customers]  id=1  →  m9-customers  (created)
  [m9-mysql-customers]  id=2  →  m9-customers  (created)
  ...
```

### Verify with curl

```bash
# Total document count:
curl 'http://localhost:9200/m9-customers/_count?pretty'

# Search all documents:
curl 'http://localhost:9200/m9-customers/_search?pretty'

# Fetch a specific document by id:
curl 'http://localhost:9200/m9-customers/_doc/1?pretty'
```

---

## 6. Script 04 — Pipeline Monitor & Live Demo

**File:** `python_scripts/module09/04_pipeline_monitor.py`

This script demonstrates the pipeline live by inserting a row into MySQL and
timing how long it takes to appear in Kafka and Elasticsearch.

### Run

```bash
# Watch the pipeline dashboard (no inserts):
python python_scripts/module09/04_pipeline_monitor.py --watch-only

# Insert a customer and watch it propagate:
python python_scripts/module09/04_pipeline_monitor.py \
  --table customers \
  --name "Yuki Nakamura" \
  --country Japan

# Insert an order:
python python_scripts/module09/04_pipeline_monitor.py \
  --table orders \
  --product "Mechanical Keyboard" \
  --amount 149.99 \
  --customer-id 1
```

### Watch-only dashboard output

```
============================================================
  Pipeline Status  [10:05:30]
============================================================

  Kafka topic message counts:
    m9-mysql-customers             5 messages
    m9-mysql-orders                5 messages

  Elasticsearch document counts:
    m9-customers                   5 documents
    m9-orders                      5 documents
```

### Insert-and-track output

```
  Inserted customers id=6
  Kafka baseline offset in 'm9-mysql-customers': 5

  Waiting for row to appear in Kafka and Elasticsearch ...
  (Make sure Script 02 and Script 03 are running in other terminals!)

  ✓ Kafka: offset 6  (lag: 3.2s)
  ✓ Elasticsearch: doc '6' in 'm9-customers'  (lag: 4.1s)

  Pipeline propagation complete.
```

---

## 7. Full Lab Walkthrough

Follow these steps in order to run the complete pipeline from scratch.

### Step 0: Start the Docker environment

```bash
cd docker
docker compose --profile pipeline up -d --build
```

Wait ~30 seconds for MySQL and Elasticsearch to become healthy.

### Step 1: Create Kafka topics

```bash
bash docker/scripts/create-topics.sh
```

Confirm with:

```bash
docker exec -it kafka1 kafka-topics.sh \
  --bootstrap-server kafka1:29092 \
  --list | grep m9
```

Expected: `m9-mysql-customers`, `m9-mysql-orders`, `m9-elasticsearch-sink`

### Step 2: Explore the source

```bash
source .venv/bin/activate
python python_scripts/module09/01_explore_mysql_source.py
```

### Step 3: Start the producer (Terminal 1)

```bash
source .venv/bin/activate
python python_scripts/module09/02_mysql_to_kafka_producer.py
```

Leave this running.

### Step 4: Start the consumer (Terminal 2)

```bash
source .venv/bin/activate
python python_scripts/module09/03_kafka_to_elasticsearch_consumer.py
```

Leave this running.

### Step 5: Observe the dashboard (Terminal 3)

```bash
source .venv/bin/activate
python python_scripts/module09/04_pipeline_monitor.py --watch-only
```

### Step 6: Insert new data and watch it propagate (Terminal 3)

```bash
python python_scripts/module09/04_pipeline_monitor.py \
  --table customers \
  --name "Raj Patel" \
  --country India
```

Watch Terminal 1 (producer) detect and publish the new row within 5 seconds.
Watch Terminal 2 (consumer) index it into Elasticsearch.
Watch the monitor report the exact latency.

---

## 8. Summary

| Script | Role | Kafka API |
|--------|------|-----------|
| `01_explore_mysql_source.py` | Inspect source schema | — |
| `02_mysql_to_kafka_producer.py` | Source connector | `KafkaProducer` |
| `03_kafka_to_elasticsearch_consumer.py` | Sink connector | `KafkaConsumer` |
| `04_pipeline_monitor.py` | Live demo / observation | `KafkaConsumer` (read-only) |

### Key patterns learned

| Pattern | Where used | Why |
|---------|-----------|-----|
| High-water mark polling | Script 02 | Capture new rows without CDC |
| JSON envelope | Script 02 | Self-describing events |
| `acks="all"` | Script 02 | Durability guarantee |
| Consumer group | Script 03 | Horizontal scalability |
| `auto_offset_reset="earliest"` | Script 03 | Replay all history on first run |
| Idempotent PUT | Script 03 | Safe redelivery |

---

## 9. Exit Criteria

You have completed Module 9 when:

- [ ] `01_explore_mysql_source.py` runs and displays both tables with seed data
- [ ] `02_mysql_to_kafka_producer.py --once` publishes all seed rows to both topics
- [ ] `kafka-console-consumer.sh` shows JSON envelopes in `m9-mysql-customers`
- [ ] `03_kafka_to_elasticsearch_consumer.py` indexes all 5 customers and 5 orders
- [ ] `curl http://localhost:9200/m9-customers/_count` returns `{"count":5,...}`
- [ ] `04_pipeline_monitor.py --table customers --name "Test User" --country Test` reports Kafka lag < 10s and ES lag < 15s

---

## 10. Further Reading

- [Kafka documentation: Consumer Groups](https://kafka.apache.org/documentation/#intro_consumers)
- [Elasticsearch: Index API](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-index_.html)
- [Debezium: MySQL CDC Connector](https://debezium.io/documentation/reference/stable/connectors/mysql.html) — what Module 10 builds on
- [kafka-python: KafkaConsumer](https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html)
