# Module 12: Kafka APIs and Monitoring Kafka

**Course:** Apache Kafka (2-Day, 14 Hours, Instructor-Led)
**Module duration:** ~75 minutes, hands-on CLI/API focused
**Position in course:** Day 2, monitoring and operations session

---

## Learning Objectives

By the end of this module, learners will be able to:

1. Identify the main Kafka APIs used by applications and operators
2. Use CLI and REST endpoints to inspect Kafka state
3. Explain basic Kafka metrics: offsets, lag, throughput, latency, and health
4. Inspect broker health using Kafka and Docker commands
5. Monitor clients using consumer group lag and performance tools
6. Run simple end-to-end monitoring checks for the pipeline

This module uses the current Docker platform. It does not require Prometheus or Grafana.

---

## 1. Kafka APIs

Kafka exposes several APIs. Some are used by applications; others are used by operators.

| API / Tool | Used for | Course examples |
|---|---|---|
| Producer API | Write records to Kafka | Module 4, Module 7 |
| Consumer API | Read records from Kafka | Module 5, Module 7 |
| Admin API | Create topics, inspect metadata, manage configs | `verify_setup.py`, CLI admin commands |
| Kafka Connect REST API | Manage connectors | Module 9 pipeline profile |
| Kafka CLI tools | Operational admin and inspection | Modules 6, 7, 11, 12 |

### 1.1 Producer API

Producer API concepts:

- `bootstrap_servers`
- serializers
- keys
- `acks`
- retries
- batching
- delivery callbacks

Course scripts:

```bash
python python_scripts/module04/01_simple_producer.py
python python_scripts/module04/03_async_producer_callbacks.py
python python_scripts/module07/01_reliable_producer.py
```

What to monitor:

- Successful sends
- Failed sends
- Send latency
- Retry count
- Throughput

### 1.2 Consumer API

Consumer API concepts:

- `group_id`
- `poll()`
- offsets
- commits
- `auto_offset_reset`
- partition assignment
- manual `assign()` and `seek()`

Course scripts:

```bash
python python_scripts/module05/01_simple_consumer.py
python python_scripts/module05/03_commits_and_offsets.py --mode manual_sync
python python_scripts/module07/02_reliable_consumer.py --max-messages 8
```

What to monitor:

- Consumer lag
- Commit behavior
- Rebalances
- Processing errors
- Throughput per consumer group

### 1.3 Kafka Connect REST API

Kafka Connect runs only with the pipeline profile:

```bash
cd docker
docker compose --profile pipeline up -d --build
```

List active connectors:

```bash
curl http://localhost:8083/connectors
```

List installed connector plugins:

```bash
curl http://localhost:8083/connector-plugins
```

Expected output is JSON:

```json
[
  {
    "class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "type": "source",
    "version": "10.9.3"
  }
]
```

PowerShell alternative:

```powershell
Invoke-RestMethod http://localhost:8083/connectors
Invoke-RestMethod http://localhost:8083/connector-plugins
```

---

## 2. Python API Monitoring Scripts

This module also includes Python versions of the monitoring checks:

```text
python_scripts/module12/
  01_kafka_api_health_check.py
  02_consumer_lag_monitor.py
```

These scripts use Kafka APIs directly from Python. They are useful when monitoring needs to be automated instead of run manually from the CLI.

### 2.1 Kafka API Health Check

Run from the repository root:

```bash
source .venv/bin/activate
python python_scripts/module12/01_kafka_api_health_check.py
```

The script checks:

- Cluster metadata
- Broker list
- Topic partitions, leaders, replicas, and ISR
- Topic end offsets
- A small produce -> consume round trip

Expected output includes:

```text
Cluster Metadata
  Brokers    : 3

Topic Metadata: m12-metrics-topic
  partition=0 leader=1 replicas=[1, 2, 3] isr=[1, 2, 3]

Producer API Probe
  OK key=probe-0 event_id=m12-a1b2c3d4 partition=1 offset=42

Consumer API Round Trip
  FOUND event_id=m12-a1b2c3d4 key=probe-0 partition=1 offset=42
```

Useful options:

```bash
python python_scripts/module12/01_kafka_api_health_check.py --messages 5
python python_scripts/module12/01_kafka_api_health_check.py --no-roundtrip
```

### 2.2 Consumer Lag Monitor

Run:

```bash
python python_scripts/module12/02_consumer_lag_monitor.py \
  --group-id m12-monitor-group \
  --topic m12-metrics-topic
```

Expected output:

```text
Partition   Committed     Log End        Lag  Status
        0           12          12          0  caught up
        1            8          11          3  behind
        2            -           4          4  no committed offset

Total lag              : 7
Partitions without commit: 1
```

Watch mode:

```bash
python python_scripts/module12/02_consumer_lag_monitor.py \
  --group-id m12-monitor-group \
  --topic m12-metrics-topic \
  --watch
```

This repeats the lag calculation every few seconds until `Ctrl+C`.

---

## 3. Metrics Basics

Kafka monitoring starts with a few core questions:

- Are brokers running?
- Are topics healthy?
- Are consumers keeping up?
- Is data flowing end to end?
- Are requests slow or failing?

### 3.1 Offset

An offset is a record's position inside a partition.

```text
Topic: orders
Partition 0
offset: 0  1  2  3  4  5
```

Kafka tracks offsets per partition, not globally per topic.

### 3.2 Log End Offset

The **log end offset** is the next offset that will be written. If log end offset is 10, the latest record is at offset 9.

### 3.3 Consumer Lag

Lag measures how far a consumer group is behind.

```text
lag = log end offset - current consumer offset
```

Example:

```text
CURRENT-OFFSET: 100
LOG-END-OFFSET: 140
LAG: 40
```

Lag means there are 40 records this group has not processed yet.

### 3.4 Throughput

Throughput is how many records or bytes move per second.

Examples:

- Producer records per second
- Consumer records per second
- Broker bytes in/out per second

### 3.5 Latency

Latency is how long an operation takes.

Examples:

- Produce request latency
- Consumer processing latency
- End-to-end latency from source to destination

---

## 4. Broker Health Checks

### 4.1 Docker Container Health

Run from `docker/`:

```bash
docker compose ps
```

Expected output:

```text
NAME      STATUS
kafka1    Up (healthy)
kafka2    Up (healthy)
kafka3    Up (healthy)
kafdrop   Up
```

How to read it:

- `healthy` means the container health check can reach the broker.
- `starting` means the broker is still booting.
- `exited` means the broker process stopped.

### 4.2 Broker API Check

```bash
docker exec kafka1 /opt/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server kafka1:29092,kafka2:29093,kafka3:29094
```

Expected output includes each broker:

```text
kafka1:29092 (id: 1 ...)
kafka2:29093 (id: 2 ...)
kafka3:29094 (id: 3 ...)
```

What it proves:

- Brokers are reachable.
- Broker ids are visible.
- The cluster can respond to Kafka protocol requests.

### 4.3 Topic Health

```bash
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:29092 \
  --describe \
  --topic m12-metrics-topic
```

Expected output:

```text
Topic: m12-metrics-topic  PartitionCount: 3  ReplicationFactor: 3
  Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
```

Check:

- Leaders exist for all partitions.
- Replicas are assigned.
- ISR contains expected brokers.

If ISR shrinks, at least one replica is not caught up or not available.

### 4.4 KRaft Controller Health

```bash
docker exec kafka1 /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server kafka1:29092 \
  describe --status
```

Important fields:

```text
LeaderId:       1
CurrentVoters:  [1,2,3]
MaxFollowerLag: 0
```

How to read it:

- `LeaderId` is the active controller.
- `CurrentVoters` should include the controller quorum members.
- `MaxFollowerLag` should normally be small.

### 4.5 Disk Usage Per Broker

```bash
docker exec kafka1 /opt/kafka/bin/kafka-log-dirs.sh \
  --bootstrap-server kafka1:29092 \
  --describe
```

Look for:

```text
"broker": 1
"logDir": "/var/lib/kafka/data"
"partition": "m12-metrics-topic-0"
"size": 1234
"offsetLag": 0
```

Useful fields:

- `size`: disk space used by a partition replica
- `offsetLag`: replica lag
- `logDir`: broker storage path

### 4.6 Container Resource Usage

```bash
docker stats kafka1 kafka2 kafka3
```

Watch:

- CPU %
- Memory usage
- Network I/O
- Block I/O

Press `Ctrl+C` to stop.

This is not Kafka-specific, but it is useful for basic broker health.

---

## 5. Client Monitoring

Client monitoring asks whether producers and consumers are healthy.

### 5.1 Produce Test Records

```bash
docker exec -it kafka1 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka1:29092 \
  --topic m12-metrics-topic
```

Type:

```text
metric test 1
metric test 2
metric test 3
```

Press `Ctrl+C`.

### 5.2 Consume With a Group

```bash
docker exec -it kafka1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka1:29092 \
  --topic m12-metrics-topic \
  --group m12-monitor-group
```

Leave this running while producing messages from another terminal.

### 5.3 Inspect Consumer Lag

```bash
docker exec kafka1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka1:29092 \
  --describe \
  --group m12-monitor-group
```

Example:

```text
GROUP              TOPIC              PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
m12-monitor-group  m12-metrics-topic  0          5               5               0
```

How to read it:

- `LAG=0`: consumer is caught up.
- `LAG>0`: records are waiting to be processed.
- No rows may appear until the group has consumed or committed offsets.

### 5.4 Producer Performance Test

```bash
docker exec kafka1 /opt/kafka/bin/kafka-producer-perf-test.sh \
  --topic m12-metrics-topic \
  --num-records 10000 \
  --record-size 100 \
  --throughput -1 \
  --producer-props bootstrap.servers=kafka1:29092 acks=all
```

Example output:

```text
10000 records sent, 45000.00 records/sec, 4.29 MB/sec, avg latency 12.4 ms
```

Useful fields:

- records/sec
- MB/sec
- average latency
- max latency

### 5.5 Consumer Performance Test

```bash
docker exec kafka1 /opt/kafka/bin/kafka-consumer-perf-test.sh \
  --bootstrap-server kafka1:29092 \
  --topic m12-metrics-topic \
  --messages 10000
```

Example output:

```text
start.time, end.time, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec
```

Useful fields:

- messages consumed
- messages/sec
- MB/sec

### 5.6 End-to-End Latency Tool

```bash
docker exec kafka1 /opt/kafka/bin/kafka-e2e-latency.sh \
  kafka1:29092 \
  m12-metrics-topic \
  100 \
  all \
  100
```

Arguments:

```text
broker_list topic num_messages producer_acks message_size_bytes
```

Example output includes latency percentiles:

```text
Avg latency: 8.7 ms
Percentiles: 50th = 7, 99th = 25
```

Use this for a quick producer -> broker -> consumer latency check.

---

## 6. Kafka Connect Monitoring

Kafka Connect is only available when the pipeline profile is running.

Start it:

```bash
cd docker
docker compose --profile pipeline up -d --build
```

### 6.1 Check Connect Is Running

```bash
curl http://localhost:8083/connectors
```

Expected output:

```json
[]
```

An empty array means Connect is running but no connectors are active.

### 6.2 Check Connector Plugins

```bash
curl http://localhost:8083/connector-plugins
```

Expected output includes connector classes such as:

```json
[
  {
    "class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "type": "source"
  },
  {
    "class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
    "type": "sink"
  }
]
```

What it proves:

- Connect REST API is reachable.
- Plugin path is working.
- Connector jars loaded successfully.

---

## 7. End-to-End Monitoring

End-to-end monitoring checks the full path from source to destination.

For this course, Module 9 provides the pipeline:

```text
MySQL -> Kafka -> Elasticsearch
```

Start the pipeline services:

```bash
cd docker
docker compose --profile pipeline up -d --build
cd ..
```

Run the producer and consumer in separate terminals:

```bash
python python_scripts/module09/02_mysql_to_kafka_producer.py
```

```bash
python python_scripts/module09/03_kafka_to_elasticsearch_consumer.py
```

### 7.1 Watch Pipeline Counts

```bash
python python_scripts/module09/04_pipeline_monitor.py --watch-only
```

Expected output:

```text
Kafka topic message counts:
  m9-mysql-customers        10 messages
  m9-mysql-orders           10 messages

Elasticsearch document counts:
  m9-customers              10 documents
  m9-orders                 10 documents
```

This confirms Kafka and Elasticsearch counts from one monitoring script.

### 7.2 Measure One Record's Propagation

```bash
python python_scripts/module09/04_pipeline_monitor.py \
  --table customers \
  --name "Module Twelve Test" \
  --country "Canada"
```

Expected output:

```text
Kafka: offset 42 (lag: 5.1s)
Elasticsearch: doc '12' in 'm9-customers' (lag: 6.2s)
Pipeline propagation complete.
```

This is an end-to-end latency check:

```text
MySQL insert -> Kafka topic -> Elasticsearch index
```

---

## 8. Monitoring Checklist

| Question | Command |
|---|---|
| Are brokers running? | `docker compose ps` |
| Can Kafka respond to protocol requests? | `kafka-broker-api-versions.sh` |
| Are topic replicas healthy? | `kafka-topics.sh --describe` |
| Is the KRaft quorum healthy? | `kafka-metadata-quorum.sh describe --status` |
| How much lag does a group have? | `kafka-consumer-groups.sh --describe` |
| How fast can producers write? | `kafka-producer-perf-test.sh` |
| How fast can consumers read? | `kafka-consumer-perf-test.sh` |
| What is end-to-end latency? | `kafka-e2e-latency.sh` or Module 9 monitor |
| Is Connect running? | `curl http://localhost:8083/connectors` |

---

## 9. What This Platform Does Not Include Yet

The current platform supports basic monitoring through Kafka CLI, Docker, Kafdrop, Connect REST, and Python scripts.

It does not yet include:

- Prometheus
- Grafana
- JMX Exporter
- Broker JMX ports exposed to the host
- Long-term metrics storage

Those can be added later with a monitoring Compose override. For this course module, the goal is to understand the core metrics and how to inspect them with the tools already available.

---

## 10. Module 12 Summary

| Concept | Key point |
|---|---|
| Kafka APIs | Producer, Consumer, Admin, Connect REST, and CLI tools |
| Offset | Position of a record inside a partition |
| Log end offset | Next offset to be written |
| Consumer lag | Records not yet processed by a group |
| Throughput | Records or bytes per second |
| Latency | Time taken for produce, consume, or end-to-end flow |
| Broker monitoring | Check health, topic ISR, KRaft quorum, log dirs, Docker stats |
| Client monitoring | Check producer/consumer behavior and group lag |
| End-to-end monitoring | Verify full source -> Kafka -> destination path |

---

## Module 12 Exit Criteria

A learner completes this module when:

- [ ] They can name the main Kafka APIs and their purpose
- [ ] They can check broker health with CLI commands
- [ ] They can describe topic ISR and consumer lag
- [ ] They can run producer and consumer performance tests
- [ ] They can run a basic end-to-end latency check
- [ ] They can inspect Kafka Connect through the REST API
- [ ] They can explain what monitoring tools are missing from the base platform and why they matter in production

---

## Further Reading

- Kafka documentation - Operations
- Kafka documentation - Monitoring
- Kafka documentation - Consumer group command
- Kafka documentation - Performance testing tools
- Kafka Connect REST API documentation
