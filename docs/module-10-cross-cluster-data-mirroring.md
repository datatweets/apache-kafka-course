# Module 10: Cross-Cluster Data Mirroring

**Course:** Apache Kafka (2-Day, 14 Hours, Instructor-Led)
**Module duration:** ~45 minutes, theory focused with CLI demonstrations
**Position in course:** Day 2, after real-time pipeline design and before administration

---

## Learning Objectives

By the end of this module, learners will be able to:

1. Explain why organizations mirror Kafka data across clusters
2. Describe active-passive, active-active, and hub-and-spoke mirroring patterns
3. Understand how MirrorMaker 2 uses Kafka Connect internally
4. Identify what is replicated: topics, records, consumer offsets, checkpoints, and heartbeats
5. Explain common failure modes in disaster recovery and migration scenarios
6. Use CLI commands to inspect topics, consumer groups, and replication-related metadata

This course platform runs one Kafka cluster. The module is therefore conceptual, with CLI commands that inspect the local cluster and show what would be configured in a real multi-cluster environment.

---

## 1. Why Mirror Kafka Data?

Cross-cluster mirroring means copying Kafka records from one Kafka cluster to another.

Common reasons:

| Use case | Description |
|---|---|
| Disaster recovery | Keep a standby cluster in another region |
| Data locality | Copy events closer to analytics or application teams |
| Cloud migration | Move topics from one Kafka platform to another |
| Cluster upgrade | Build a new cluster, mirror traffic, then cut over |
| Compliance | Keep a regional copy of selected event streams |

Mirroring is not the same as broker replication. Broker replication copies partitions inside one cluster. Cross-cluster mirroring copies data between separate Kafka clusters.

---

## 2. Common Mirroring Topologies

### 2.1 Active-Passive

One cluster handles writes. A second cluster receives mirrored data and is used only during failover.

```text
Applications -> Cluster A -> mirrored topics -> Cluster B
                     active                    standby
```

Best for disaster recovery because ownership is simple.

### 2.2 Active-Active

Two clusters both accept writes and mirror selected topics to each other.

```text
Applications in region A -> Cluster A <----> Cluster B <- Applications in region B
```

This is harder. You must avoid loops, duplicate processing, key conflicts, and ambiguous ownership.

### 2.3 Hub-and-Spoke

Regional clusters mirror selected data into a central analytics cluster.

```text
Cluster A \
Cluster B  ---> Central analytics cluster
Cluster C /
```

Useful when many regions produce data but one platform performs reporting or machine learning.

---

## 3. MirrorMaker 2

MirrorMaker 2 is Kafka's built-in cross-cluster replication framework. It runs on Kafka Connect.

Key internal connectors:

| Connector | Purpose |
|---|---|
| `MirrorSourceConnector` | Copies records from source topics to target topics |
| `MirrorCheckpointConnector` | Emits consumer group checkpoint information |
| `MirrorHeartbeatConnector` | Emits heartbeat records to prove connectivity |

MirrorMaker 2 normally creates remote topics with a source-cluster prefix:

```text
source topic: orders
target topic: primary.orders
```

That prefix prevents naming collisions and helps identify where mirrored records came from.

---

## 4. What Gets Mirrored?

| Item | Mirrored? | Notes |
|---|---|---|
| Topic records | Yes | Main replication payload |
| Topic names | Yes | Often renamed with source alias prefix |
| Topic configs | Optionally | Depends on MirrorMaker 2 config |
| ACLs | Optionally | Common in secured production clusters |
| Consumer offsets | Indirectly | Checkpoints help translate source offsets to target offsets |
| Consumer group state | Partially | Groups do not simply "move"; failover needs planning |
| Broker metadata | No | Clusters remain independent |

Important: mirrored data is usually **at-least-once**. Downstream systems should tolerate duplicates.

---

## 5. Example MirrorMaker 2 Configuration

This is an illustrative configuration for two clusters named `primary` and `backup`.

```properties
clusters = primary, backup

primary.bootstrap.servers = primary-kafka1:9092,primary-kafka2:9092,primary-kafka3:9092
backup.bootstrap.servers = backup-kafka1:9092,backup-kafka2:9092,backup-kafka3:9092

primary->backup.enabled = true
primary->backup.topics = orders|payments|customers
primary->backup.groups = .*

replication.factor = 3
checkpoints.topic.replication.factor = 3
heartbeats.topic.replication.factor = 3
offset-syncs.topic.replication.factor = 3

sync.topic.configs.enabled = true
sync.topic.acls.enabled = false
emit.checkpoints.enabled = true
emit.heartbeats.enabled = true
```

In a real deployment, this file is passed to `connect-mirror-maker.sh`.

---

## 6. CLI Demonstrations on This Course Cluster

These commands do not create a second cluster. They help learners inspect the metadata that matters before planning mirroring.

### 6.1 List Topics

```bash
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:29092 \
  --list
```

Look for topics you might mirror:

```text
m9-mysql-customers
m9-mysql-orders
m13-stream-output
m13-wordcount-output
```

### 6.2 Describe a Candidate Topic

```bash
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:29092 \
  --describe \
  --topic m9-mysql-orders
```

Pay attention to:

- partition count
- replication factor
- leader distribution
- in-sync replicas

Target clusters should usually have compatible capacity and partitioning.

### 6.3 Inspect Consumer Groups

```bash
docker exec kafka1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka1:29092 \
  --list
```

Describe a group:

```bash
docker exec kafka1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka1:29092 \
  --describe \
  --group m13-monitor-group
```

Consumer groups matter because failover is not just about copying records. Applications also need to know where to resume.

### 6.4 Inspect Kafka Connect Availability

MirrorMaker 2 runs on Kafka Connect. Start the pipeline profile first if Connect is not running:

```bash
cd docker
docker compose --profile pipeline up -d --build
```

Check Connect:

```bash
curl http://localhost:8083/connectors
```

Expected output may be an empty list:

```json
[]
```

That means Connect is reachable, even if no connectors are configured.

### 6.5 Check Kafka Connect Internal Topics

```bash
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:29092 \
  --list | grep _connect
```

Expected topics:

```text
_connect-configs
_connect-offsets
_connect-status
```

These topics store Kafka Connect's own state. MirrorMaker 2 connector state is also managed by Connect.

---

## 7. Failover Considerations

Before failing over from source to target, teams must answer:

- Which cluster accepts new writes?
- Are producers switched by DNS, config, or deployment?
- Are consumers starting from translated offsets or from earliest/latest?
- Are duplicate records acceptable?
- Are topic configs and ACLs already present on the target?
- Is the target cluster sized for production traffic?

A clean disaster recovery plan is mostly operational discipline, not just a replication tool.

---

## 8. Key Takeaways

- Cross-cluster mirroring copies data between independent Kafka clusters.
- MirrorMaker 2 is built on Kafka Connect and uses source, checkpoint, and heartbeat connectors.
- Active-passive is simpler than active-active.
- Mirroring is usually at-least-once, so duplicate-tolerant consumers are important.
- Consumer offset failover requires checkpoint planning.
- This course cluster can demonstrate the metadata and CLI inspection workflow, but a real mirroring lab requires a second Kafka cluster.
