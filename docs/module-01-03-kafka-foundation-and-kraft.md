# Module 1: Apache Kafka — Introduction

**Course:** Apache Kafka (2-Day, 14 Hours, Instructor-Led)
**Module duration:** ~90 minutes including hands-on platform start
**Position in course:** Day 1, first session (9:00 am – 10:15 am)

---

## Learning Objectives

By the end of this module, learners will be able to:

1. Explain what a messaging system is and why one is needed in modern architectures
2. Describe why Apache Kafka was created and where it sits in the data ecosystem
3. Identify the core components of Kafka: brokers, topics, producers, consumers, and partitions
4. Understand the role that ZooKeeper historically played — and why Kafka 4 replaces it with KRaft
5. Start and validate the course Docker platform on either macOS or Windows

---

## 1. What Is a Messaging System?

### The problem that messaging systems solve

Modern applications are built from many independent services: a web API, a database, a recommendation engine, an analytics dashboard, a notification service. Each service has its own code, its own data, and its own deployment lifecycle. They need to share data in real time, but connecting them directly creates brittle point-to-point dependencies.

A **messaging system** sits between producers of data and consumers of data, decoupling them so that:

- A producer does not need to know how many consumers exist, or whether they are running
- A consumer does not need to know where the data came from
- Either side can be scaled, restarted, or replaced independently

### Types of messaging models

| Model | Description | Example |
|---|---|---|
| Point-to-Point (Queue) | One producer, one consumer per message | Task queues, job workers |
| Publish-Subscribe (Pub-Sub) | One producer, many independent consumers | Event broadcasting |
| Log-based streaming | Ordered, retained, replayable stream of events | Apache Kafka |

Kafka takes the log-based model. Events are written to a durable, ordered log. Every consumer reads the log independently at its own pace. This is the fundamental insight that separates Kafka from traditional message queues like RabbitMQ or ActiveMQ.

### Key properties messaging systems provide

- **Asynchronous communication** — sender and receiver are decoupled in time
- **Buffering** — absorbs traffic spikes without dropping data
- **Fan-out** — one event delivered to many consumers independently
- **Replay** — consumers can re-read past data
- **Durability** — messages survive failures

---

## 2. Why Apache Kafka?

### History

Apache Kafka was created at LinkedIn around 2010 by Jay Kreps, Neha Narkhede, and Jun Rao. LinkedIn needed to collect and distribute billions of activity events per day across its platform. Existing messaging systems could not handle that throughput reliably and cheaply.

LinkedIn open-sourced Kafka in 2011. It became an Apache Software Foundation top-level project in 2012. It is now one of the most widely deployed distributed systems in the world.

### What Kafka solves better than alternatives

| Concern | Kafka | Traditional queues |
|---|---|---|
| Throughput | Millions of messages per second | Thousands to hundreds of thousands |
| Retention | Configurable time or size | Message deleted after consumption |
| Replay | Any consumer can re-read | Once consumed, gone |
| Consumer groups | Multiple independent groups | Competing consumers share a queue |
| Durability | Replication across brokers | Varies |
| Ordering | Ordered per partition | Limited guarantees |

### Where Kafka is used today

- Real-time analytics pipelines (user clicks, transactions, sensor readings)
- Change Data Capture (CDC) from databases to downstream systems
- Log aggregation from distributed services
- Event sourcing for microservices
- Stream processing with Kafka Streams or Apache Flink
- Data integration replacing traditional ETL

---

## 3. Apache Kafka Basics

### Core components

```
Producers  ──►  Topics (inside Brokers)  ──►  Consumers
```

**Producer**
An application that writes (publishes) events to Kafka. A producer sends each event to a named topic. The producer decides which partition within the topic receives the event.

**Broker**
A Kafka server. It receives events from producers, stores them durably on disk, and serves them to consumers. In this course the platform runs three brokers (`kafka1`, `kafka2`, `kafka3`).

**Cluster**
A group of brokers working together. One broker per partition acts as the leader; the others hold replicas.

**Topic**
A named, ordered, durable log of events. A topic has one or more partitions.

**Partition**
The unit of parallelism and ordering in Kafka. Within a partition, events are strictly ordered by offset. Across partitions, ordering is not guaranteed.

```
Topic: orders
  Partition 0:  [offset 0] [offset 1] [offset 2] [offset 3] ...
  Partition 1:  [offset 0] [offset 1] [offset 2] ...
  Partition 2:  [offset 0] [offset 1] ...
```

**Offset**
A sequential number that identifies each event's position within a partition. Consumers track their position using offsets.

**Consumer**
An application that reads events from a topic. A consumer tracks its own offset, so it knows where to resume after a restart.

**Consumer Group**
A named group of consumers that cooperate to read a topic. Each partition is assigned to exactly one consumer in the group, enabling parallel processing at scale.

**Replication**
Each partition has a configurable replication factor. In this course the factor is 3, meaning each partition leader has two replicas on other brokers. If a broker fails, a replica is promoted to leader.

### The event (record) structure

Every Kafka message is a record with these fields:

| Field | Description |
|---|---|
| Key | Optional. Used to route to a partition and for compaction. Can be null. |
| Value | The payload. Kafka treats it as bytes; your application decides the format (JSON, Avro, Protobuf, plain string). |
| Timestamp | When the event was produced (or broker-assigned). |
| Headers | Optional key-value metadata, not part of the value payload. |
| Topic + Partition + Offset | Set by Kafka after the event is written. |

### Kafka data flow — step by step

```
1. Producer connects to a broker (bootstrap server)
2. Producer sends a record to topic "orders"
3. Broker assigns it to a partition based on the key (or round-robin if key is null)
4. Broker writes it to the partition log on disk
5. Broker replicates it to follower brokers
6. Broker sends acknowledgement to producer
7. Consumer polls the broker for new records
8. Consumer processes the record
9. Consumer commits its offset
```

---

## 4. The Data Ecosystem

Kafka connects many different systems in a modern data platform:

```
┌─────────────────────────────────────────────────────────────┐
│                    SOURCE SYSTEMS                           │
│  Relational DB  │  Web/App Events  │  IoT Sensors   │  Logs │
└────────┬────────┴────────┬─────────┴───────┬────────┴───┬───┘
         │                 │                 │            │
         ▼                 ▼                 ▼            ▼
┌─────────────────────────────────────────────────────────────┐
│                        APACHE KAFKA                         │
│              (durable, ordered, scalable log)               │
└────────┬────────┬────────────┬──────────┬───────────────────┘
         │        │            │          │
         ▼        ▼            ▼          ▼
   Analytics  Microservice  Search   Data Warehouse
   (Spark,    (consumer     (Elastic  (BigQuery,
    Flink)     groups)      search)   Snowflake)
```

Kafka's role is to act as the **central nervous system** of the data platform — ingesting from any source, retaining reliably, and distributing to any number of downstream systems at their own pace.

### Kafka in the pipeline labs of this course

This course includes a full pipeline demo using:

```
MySQL  ──►  Python producer  ──►  Kafka  ──►  Python consumer  ──►  Elasticsearch
```

That pipeline is in `python_pipeline/` and is covered in Modules 8 and 9.

---

## 5. Apache Kafka Workflow

### Producer workflow

1. Application creates a `KafkaProducer` with a bootstrap server address
2. Application calls `producer.send(topic, key, value)`
3. Kafka client serialises the key and value to bytes
4. Partitioner decides which partition receives the record (hash of key, or round-robin)
5. The record is batched locally for efficiency
6. Batch is sent to the leader broker for that partition
7. Leader writes to its log and replicates to followers
8. Broker sends acknowledgement (controlled by `acks` config)

### Consumer workflow

1. Application creates a `KafkaConsumer` and subscribes to one or more topics
2. Consumer joins a consumer group (or works standalone)
3. Kafka assigns partitions to this consumer
4. Consumer calls `consumer.poll()` in a loop
5. Kafka returns a batch of records from the consumer's current offset
6. Application processes each record
7. Consumer commits the offset (auto or manual)
8. On the next poll, consumption resumes from the committed offset

### Retention and replay

Kafka retains events for a configurable time (`log.retention.hours`) or size. This course platform defaults to 168 hours (7 days). During that window, any consumer can re-read from offset 0 — regardless of what other consumers have already read. This makes Kafka fundamentally different from a queue.

---

## 6. ZooKeeper — Historical Role and Why It Is Not Used in This Course

> **Important notice for learners:** This section explains ZooKeeper's historical role. In Kafka 4, ZooKeeper is no longer available. This course runs Kafka 4 in KRaft mode. Any documentation, tutorial, or book that references ZooKeeper-based Kafka is describing a version prior to Kafka 4. Do not attempt to add ZooKeeper to the platform used in this course.

### What ZooKeeper was

Apache ZooKeeper is a distributed coordination service — a separate cluster that provides a shared, consistent, hierarchical key-value store. Before KRaft, Kafka used ZooKeeper to:

| Function | What ZooKeeper stored |
|---|---|
| Cluster membership | Which brokers are alive |
| Controller election | Which broker is the active controller |
| Topic metadata | Partition assignments, leader elections |
| Consumer group offsets | Where each consumer group is up to (old clients) |
| ACLs and configs | Security rules, topic configurations |

### Why ZooKeeper became a problem

Running ZooKeeper alongside Kafka meant:

- **Two systems to operate:** separate JVM processes, separate configuration, separate monitoring, separate failure modes
- **Scalability ceiling:** ZooKeeper stored all partition metadata in memory; clusters with millions of partitions hit limits
- **Slow controller failover:** The controller election round-trip through ZooKeeper could take 30+ seconds
- **Operational complexity:** Learners and operators had to understand two distributed systems instead of one

### KRaft — Kafka Raft Metadata mode

KRaft (Kafka Raft) was introduced in KIP-500 and moves all cluster metadata management inside Kafka itself, using the Raft consensus algorithm.

In KRaft mode:

- A subset of brokers act as **controllers** (the quorum)
- Controllers store the cluster metadata log internally — just another Kafka log
- The active controller is elected via Raft, not ZooKeeper
- Failover is seconds rather than tens of seconds
- No ZooKeeper process, no ZooKeeper ports, no ZooKeeper configuration

### How this course platform implements KRaft

In this course's `docker/docker-compose.yml`, every broker runs with combined `broker,controller` roles:

```yaml
KAFKA_PROCESS_ROLES: broker,controller
KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka1:9192,2@kafka2:9193,3@kafka3:9194
```

All three brokers participate in the controller quorum. There is no ZooKeeper container anywhere in `docker-compose.yml`.

### ZooKeeper timeline (for context)

| Kafka version | ZooKeeper status |
|---|---|
| 0.8 – 2.8 | Required, no alternative |
| 2.8 – 3.x | KRaft available as early access / preview |
| 3.3 – 3.7 | KRaft production-ready, ZooKeeper deprecated |
| 4.0 | ZooKeeper mode removed |

**If you run `docker exec kafka1 ... --zookeeper` on this platform, the flag is not recognised. Use `--bootstrap-server localhost:9092` instead.**

> Module 3 in the original course outline covered ZooKeeper operations. Because this course runs Kafka 4, Module 3 has been updated to cover KRaft architecture — controller quorum, metadata log, and leader election — which is the current and only model for Kafka 4.

---

## 7. Course Platform Overview

This course runs entirely inside Docker so that learners on macOS and Windows get identical broker behaviour. There is nothing to install on the host OS except Docker Desktop, Python, and Git.

### What the platform provides

| Container | Purpose | Host port |
|---|---|---|
| `kafka1` | Broker + controller (node 1) | `9092` |
| `kafka2` | Broker + controller (node 2) | `9093` |
| `kafka3` | Broker + controller (node 3) | `9094` |
| `kafdrop` | Web UI — browse topics and messages | `9000` |
| `mysql` | Source database for pipeline labs | `3307` (pipeline profile) |
| `elasticsearch` | Sink for pipeline labs | `9200` (pipeline profile) |
| `kafka-connect` | Connector framework for pipeline labs | `8083` (pipeline profile) |

MySQL, Elasticsearch, and Kafka Connect are only started when you add `--profile pipeline`. All core Kafka and Kafdrop labs use only the four core containers.

### Cluster configuration highlights (from `docker/.env`)

```env
KAFKA_VERSION=4.0.0
CLUSTER_ID=4L6g3lTnSsmthwI408djhg    # fixed — do not change after first run
KAFKA_HEAP=512m
KAFKA_HEAP_MIN=256m
LOG_RETENTION_HOURS=168
```

For laptops with limited RAM, open `docker/.env` and reduce:

```env
KAFKA_HEAP=384m
KAFKA_HEAP_MIN=256m
```

---

## 8. Hands-On Lab: Start the Kafka Platform

### Prerequisites

Confirm before starting:

- Docker Desktop is installed and running
- Python 3.9 or newer is installed
- Git is installed
- Repository is cloned locally

### Step 1 — Navigate to the repository root

**macOS (Terminal):**

```bash
cd /path/to/apache-kafka-course
```

**Windows (PowerShell):**

```powershell
cd C:\path\to\apache-kafka-course
```

### Step 2 — Create the Python virtual environment

**macOS:**

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

**Windows PowerShell:**

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

If PowerShell blocks the activation script, run this once and then retry:

```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
```

### Step 3 — Start the core Kafka platform

**macOS and Windows PowerShell (same command):**

```bash
cd docker
docker compose up -d
```

This downloads images on first run (may take a few minutes). After images are ready, Kafka starts with a health check that can take up to 45 seconds per broker.

### Step 4 — Confirm all containers are healthy

```bash
docker compose ps
```

Expected output — all four services showing `healthy`:

```
NAME        IMAGE                       STATUS
kafka1      apache/kafka:4.0.0          Up (healthy)
kafka2      apache/kafka:4.0.0          Up (healthy)
kafka3      apache/kafka:4.0.0          Up (healthy)
kafdrop     obsidiandynamics/kafdrop    Up
```

If a broker shows `starting` instead of `healthy`, wait 30 seconds and run `docker compose ps` again. Brokers synchronise their controller quorum on first boot.

### Step 5 — Open Kafdrop

Open a browser:

```
http://localhost:9000
```

Kafdrop shows the cluster overview with three brokers. There are no topics yet — you create them in the next step.

### Step 6 — Create course topics

From the `docker/` directory:

**macOS / Git Bash:**

```bash
bash scripts/create-topics.sh
```

**Windows PowerShell:**

```powershell
powershell -ExecutionPolicy Bypass -File scripts/create-topics.ps1
```

This creates all module topics (`m4-*`, `m5-*`, `m6-*`, through `m13-*`) with 3 partitions and replication factor 3.

Refresh Kafdrop. All course topics should appear.

### Step 7 — Verify the full setup

Go back to the repository root:

**macOS:**

```bash
cd ..
source .venv/bin/activate
python docker/scripts/verify_setup.py
```

**Windows PowerShell:**

```powershell
cd ..
.\.venv\Scripts\Activate.ps1
python docker\scripts\verify_setup.py
```

The script checks:

- Python version and dependencies
- Docker container health
- Kafka port reachability
- Live produce and consume round-trip test
- Kafdrop availability
- Cluster metadata (3 brokers, replication state)

A passing run ends with no critical failures. Warnings about optional pipeline services are expected at this stage.

### Step 8 — Run a first manual produce and consume

Produce a test message directly inside `kafka1`:

```bash
docker exec -it kafka1 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka1:29092 \
  --topic m4-simple-topic
```

Type a message and press Enter, for example:

```
Hello Kafka 4 with KRaft
```

Press `Ctrl+C` to exit the producer.

Consume it back:

```bash
docker exec -it kafka1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka1:29092 \
  --topic m4-simple-topic \
  --from-beginning
```

You should see `Hello Kafka 4 with KRaft` printed. Press `Ctrl+C` to exit.

This confirms an end-to-end produce → store → consume cycle on your cluster.

---

## 9. Useful Commands Reference

### List all topics

```bash
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:29092 --list
```

### Describe a topic

```bash
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:29092 \
  --describe --topic m4-simple-topic
```

Sample output shows leader, replicas, and ISR (in-sync replicas) per partition — all on KRaft-managed brokers, no ZooKeeper address in any command.

### Stop the platform

```bash
cd docker
docker compose down
```

To also delete all data volumes (clean reset):

```bash
docker compose down -v
```

---

## 10. Module 1 Summary

| Concept | Key Point |
|---|---|
| Messaging system | Decouples producers and consumers; enables async, durable, scalable data flow |
| Why Kafka | High throughput, durable retention, replay, consumer groups, ecosystem |
| Broker | Kafka server storing and serving topic partitions |
| Topic | Named, ordered, durable log split into partitions |
| Partition | Unit of ordering and parallelism; each has a leader and replicas |
| Consumer group | Multiple consumers sharing partition assignments for parallel processing |
| ZooKeeper | Historical dependency for cluster metadata; removed in Kafka 4 |
| KRaft | Built-in Raft consensus replacing ZooKeeper; all cluster metadata lives inside Kafka |
| This platform | 3-broker KRaft cluster on Docker; runs identically on macOS and Windows |

---

## Module 1 Exit Criteria

A learner is ready for Module 2 when:

- [ ] `docker compose ps` shows `kafka1`, `kafka2`, `kafka3`, and `kafdrop` as healthy
- [ ] Kafdrop opens at `http://localhost:9000` and shows 3 brokers
- [ ] Topic creation script completed without failures
- [ ] `verify_setup.py` passes with no critical errors
- [ ] Manual console produce and consume round-trip succeeded
- [ ] Learner can explain in their own words why this course uses KRaft and not ZooKeeper

