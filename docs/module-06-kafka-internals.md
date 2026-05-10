# Module 6: Kafka Internals

**Course:** Apache Kafka (2-Day, 14 Hours, Instructor-Led)
**Module duration:** ~75 minutes including hands-on lab
**Position in course:** Day 1, fifth session (after Module 5 - Consumers)

---

## Learning Objectives

By the end of this module, learners will be able to:

1. Explain how brokers form a Kafka cluster in KRaft mode
2. Describe the controller's role in metadata management and leader election
3. Read topic metadata: partitions, leaders, replicas, and ISR
4. Explain what happens when a broker fails and rejoins
5. Describe how producer and consumer requests are routed to partition leaders
6. Inspect Kafka's physical log storage inside broker containers

---

## 1. Why Kafka Internals Matter

Kafka looks simple from client code:

```python
producer.send("orders", value=event)
consumer.poll(timeout_ms=1000)
```

Behind those calls, Kafka is coordinating brokers, partition leaders, replicas, disk logs, client metadata, and failure recovery.

For application developers and data engineers, the goal is not to memorize Kafka source code. The goal is to understand enough internals to answer practical questions:

- Why does a topic have multiple partitions?
- Why does each partition have one leader?
- What is a replica?
- What does ISR mean?
- What happens if a broker stops?
- Why can Kafka continue running after a broker failure?
- Where are records physically stored?
- Why do reliable producers use `acks="all"`?

Module 7 builds directly on this module. Reliable data delivery only makes sense after learners understand leaders, replicas, ISR, and broker failure behavior.

---

## 2. Cluster Membership

A Kafka **cluster** is a group of brokers working together.

In this course, the Docker platform runs three Kafka brokers:

| Broker | Container | Host client address | Internal Docker address |
|---|---|---|---|
| Broker 1 | `kafka1` | `localhost:9092` | `kafka1:29092` |
| Broker 2 | `kafka2` | `localhost:9093` | `kafka2:29093` |
| Broker 3 | `kafka3` | `localhost:9094` | `kafka3:29094` |

Each broker has a unique node id:

```yaml
KAFKA_NODE_ID: 1
KAFKA_NODE_ID: 2
KAFKA_NODE_ID: 3
```

All three brokers run in KRaft combined mode:

```yaml
KAFKA_PROCESS_ROLES: broker,controller
```

This means each node can store data as a broker and also participate in the controller quorum. There is no ZooKeeper in Kafka 4.

### Check running cluster members

Run from the `docker/` directory:

```bash
docker compose ps
```

Expected output looks like:

```text
NAME      IMAGE                STATUS
kafka1    apache/kafka:4.0.0   Up (healthy)
kafka2    apache/kafka:4.0.0   Up (healthy)
kafka3    apache/kafka:4.0.0   Up (healthy)
kafdrop   ...                  Up
```

How to read it:

- `Up` means the container process is running.
- `healthy` means the container health check can successfully talk to the broker.
- If a broker is `starting`, wait a short time and run the command again.
- If a broker is missing or exited, the cluster has lost a member.

---

## 3. The Controller

The **controller** is responsible for cluster metadata decisions.

In Kafka 4 KRaft mode, controllers are part of Kafka itself. They store and replicate metadata using a Raft quorum.

The controller manages:

- Broker membership
- Topic metadata
- Partition leadership
- Leader election after failures
- Configuration changes

Only one controller is active at a time, but the controller quorum keeps metadata replicated. If the active controller fails, another controller can take over.

### Inspect the KRaft metadata quorum

Run:

```bash
docker exec kafka1 /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server kafka1:29092 \
  describe --status
```

Example output:

```text
ClusterId:              4L6g3lTnSsmthwI408djhg
LeaderId:               1
LeaderEpoch:            8
HighWatermark:          231
MaxFollowerLag:         0
MaxFollowerLagTimeMs:   0
CurrentVoters:          [1,2,3]
CurrentObservers:       []
```

How to read it:

- `ClusterId` identifies this Kafka cluster.
- `LeaderId` is the current active controller node.
- `CurrentVoters` are the controller quorum members.
- `HighWatermark` is the replicated metadata log position.
- `MaxFollowerLag` shows whether controller voters are behind the active leader.

If `CurrentVoters` shows `[1,2,3]`, all three brokers are participating in the controller quorum.

---

## 4. Partitions, Leaders, Replicas, and ISR

A Kafka topic is split into partitions. Each partition has:

- One **leader** replica
- Zero or more **follower** replicas
- A set of **in-sync replicas** (ISR)

The leader handles reads and writes for that partition. Followers copy data from the leader.

```
Topic: m6-replication-topic

Partition 0
  Leader: broker 1
  Replicas: broker 1, broker 2, broker 3
  ISR: broker 1, broker 2, broker 3
```

### Key terms

| Term | Meaning |
|---|---|
| Partition | One ordered log inside a topic |
| Leader | Broker currently handling reads/writes for a partition |
| Replica | A copy of the partition on a broker |
| Follower | Replica that copies data from the leader |
| ISR | In-sync replicas that are caught up enough to be eligible for safe writes/failover |

In this course, topics are created with 3 partitions and replication factor 3.

---

## 5. Practical Lab: Observe Replication and Broker Failure

This lab makes Kafka internals visible using only Docker and Kafka CLI commands.

You will:

1. Describe a replicated topic
2. Produce records
3. Stop one broker
4. Observe leader and ISR changes
5. Confirm Kafka still works
6. Restart the broker
7. Observe ISR recovery

### Prerequisites

Start the platform:

```bash
cd docker
docker compose up -d
```

Create course topics:

```bash
bash scripts/create-topics.sh
```

The topic used in this lab is:

```text
m6-replication-topic
```

It is created with:

- 3 partitions
- Replication factor 3
- One replica on each broker

---

## 6. Step 1 - Describe the Topic

Run:

```bash
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:29092 \
  --describe \
  --topic m6-replication-topic
```

Example output:

```text
Topic: m6-replication-topic  TopicId: abc123  PartitionCount: 3  ReplicationFactor: 3
  Topic: m6-replication-topic  Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
  Topic: m6-replication-topic  Partition: 1  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
  Topic: m6-replication-topic  Partition: 2  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2
```

The exact leader assignment may differ on your machine. That is normal.

How to read it:

- `PartitionCount: 3` means the topic has three independent logs.
- `ReplicationFactor: 3` means each partition has three copies.
- `Leader: 1` means broker 1 currently handles reads/writes for that partition.
- `Replicas: 1,2,3` means brokers 1, 2, and 3 all store a copy.
- `Isr: 1,2,3` means all three replicas are in sync.

This one command shows the core of Kafka's distributed storage model.

---

## 7. Step 2 - Produce Messages

Run:

```bash
docker exec -it kafka1 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka1:29092 \
  --topic m6-replication-topic
```

Type a few messages:

```text
message before failure 1
message before failure 2
message before failure 3
```

Press `Ctrl+C` to stop the producer.

What happens internally:

1. The producer contacts `kafka1:29092` as a bootstrap broker.
2. Kafka returns metadata for the topic.
3. For each partition, the producer learns which broker is the leader.
4. The producer sends records to partition leaders.
5. Leaders write records to disk.
6. Followers replicate records from leaders.
7. The broker acknowledges the producer.

The bootstrap broker is only the entry point. After metadata discovery, clients talk to the correct partition leaders.

---

## 8. Step 3 - Stop One Broker

Stop `kafka2`:

```bash
docker compose stop kafka2
```

Expected output:

```text
[+] Stopping 1/1
 ✔ Container kafka2  Stopped
```

What this simulates:

- A broker process is no longer available.
- Any partitions led by broker 2 need a new leader.
- Replicas on broker 2 leave the ISR.
- The cluster should continue running because two brokers remain.

---

## 9. Step 4 - Describe the Topic Again

Run:

```bash
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:29092 \
  --describe \
  --topic m6-replication-topic
```

Example output after stopping broker 2:

```text
Topic: m6-replication-topic  TopicId: abc123  PartitionCount: 3  ReplicationFactor: 3
  Topic: m6-replication-topic  Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,3
  Topic: m6-replication-topic  Partition: 1  Leader: 3  Replicas: 2,3,1  Isr: 3,1
  Topic: m6-replication-topic  Partition: 2  Leader: 3  Replicas: 3,1,2  Isr: 3,1
```

Your exact output may differ, but these are the important changes:

- Broker `2` is no longer in `Isr`.
- If broker `2` was a leader, another broker became leader.
- `Replicas` still lists `2` because broker 2 is still assigned as a replica.
- `Isr` only lists replicas currently in sync and available.

This is the most important output in the lab.

It shows:

- Kafka detects broker failure.
- The controller elects new leaders.
- The cluster keeps topic metadata.
- Replication state changes from `1,2,3` to the surviving brokers.

---

## 10. Step 5 - Produce While One Broker Is Down

Run:

```bash
docker exec -it kafka1 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka1:29092 \
  --topic m6-replication-topic
```

Type:

```text
message during broker failure 1
message during broker failure 2
```

Press `Ctrl+C` to stop.

Why this still works:

- The topic has replication factor 3.
- Two brokers are still alive.
- The course platform uses `min.insync.replicas=2`.
- The remaining ISR still has two brokers.

This prepares learners for Module 7. In Module 7, `acks="all"` will mean the producer waits for the in-sync replicas required by the broker configuration.

---

## 11. Step 6 - Consume Messages

Run:

```bash
docker exec -it kafka1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka1:29092 \
  --topic m6-replication-topic \
  --from-beginning
```

Expected output includes messages produced before and during the broker failure:

```text
message before failure 1
message before failure 2
message before failure 3
message during broker failure 1
message during broker failure 2
```

The order may vary across the full output because the topic has multiple partitions. Kafka guarantees order within a partition, not across partitions.

What this proves:

- Kafka continued storing records while one broker was down.
- Consumers can still read from current partition leaders.
- Clients do not need to know manually which broker became leader.

Press `Ctrl+C` to stop the consumer.

---

## 12. Step 7 - Restart the Broker

Start broker 2 again:

```bash
docker compose start kafka2
```

Expected output:

```text
[+] Running 1/1
 ✔ Container kafka2  Started
```

Wait 15-30 seconds for the broker to rejoin and catch up.

Then run:

```bash
docker compose ps
```

Expected output:

```text
NAME      STATUS
kafka1    Up (healthy)
kafka2    Up (healthy)
kafka3    Up (healthy)
```

Broker 2 is now a cluster member again.

---

## 13. Step 8 - Observe ISR Recovery

Describe the topic again:

```bash
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:29092 \
  --describe \
  --topic m6-replication-topic
```

Expected output after recovery:

```text
Topic: m6-replication-topic  TopicId: abc123  PartitionCount: 3  ReplicationFactor: 3
  Topic: m6-replication-topic  Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,3,2
  Topic: m6-replication-topic  Partition: 1  Leader: 3  Replicas: 2,3,1  Isr: 3,1,2
  Topic: m6-replication-topic  Partition: 2  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2
```

How to read it:

- Broker `2` has returned to `Isr`.
- It caught up with records written while it was offline.
- Leaders may or may not move back automatically. That depends on leader election behavior and preferred replica settings.
- The important point is that all replicas are in sync again.

If `2` does not appear immediately in ISR, wait a little longer and run the describe command again.

---

## 14. Request Processing

Kafka request processing can be explained through the behavior you just observed.

### Produce request path

```
Producer
  -> bootstrap broker
  -> metadata response
  -> partition leader
  -> leader appends to log
  -> followers replicate
  -> acknowledgement returned
```

The producer does not permanently send everything to the bootstrap broker. It uses the bootstrap broker to discover metadata, then sends produce requests to the leaders for the target partitions.

### Fetch request path

```
Consumer
  -> bootstrap broker
  -> metadata response
  -> partition leader
  -> fetch records from log
  -> return batch to consumer
```

Consumers also fetch from partition leaders.

### What happens after leader change?

When a broker fails:

1. The controller notices the broker is unavailable.
2. Partitions led by that broker need new leaders.
3. New leaders are chosen from the ISR.
4. Clients refresh metadata.
5. Producers and consumers continue using the new leaders.

This is why Kafka clients use `bootstrap_servers` for discovery, not as the permanent destination for every request.

Deeper request-thread metrics are useful, but they belong better in Module 12 monitoring. For Module 6, it is enough to understand the observable flow: clients discover metadata, then communicate with partition leaders.

---

## 15. Physical Storage

Kafka stores records on disk as partition logs. Each broker stores the partitions assigned to it under its log directory.

In this Docker platform, each broker uses:

```text
/var/lib/kafka/data
```

### Inspect broker storage

Run:

```bash
docker exec kafka1 ls -lah /var/lib/kafka/data
```

Example output:

```text
drwxr-xr-x  4 appuser appuser 4.0K .
drwxr-xr-x  3 appuser appuser 4.0K ..
drwxr-xr-x  2 appuser appuser 4.0K m6-replication-topic-0
drwxr-xr-x  2 appuser appuser 4.0K m6-isr-topic-2
drwxr-xr-x  2 appuser appuser 4.0K __consumer_offsets-12
```

How to read it:

- Each topic partition appears as a directory.
- `m6-replication-topic-0` means topic `m6-replication-topic`, partition `0`.
- Internal Kafka topics such as `__consumer_offsets` are stored the same way.
- Each broker only stores partitions for which it has a replica.

### Inspect log directory metadata

Run:

```bash
docker exec kafka1 /opt/kafka/bin/kafka-log-dirs.sh \
  --bootstrap-server kafka1:29092 \
  --describe
```

Example output is JSON-like and may be long:

```text
{
  "brokers": [
    {
      "broker": 1,
      "logDirs": [
        {
          "logDir": "/var/lib/kafka/data",
          "partitions": [
            {
              "partition": "m6-replication-topic-0",
              "size": 1234,
              "offsetLag": 0,
              "isFuture": false
            }
          ]
        }
      ]
    }
  ]
}
```

How to read it:

- `broker` is the broker id.
- `logDir` is the physical storage location.
- `partition` is the topic partition stored there.
- `size` is how much disk space the partition uses.
- `offsetLag` shows whether a replica is behind.

### Optional: inspect files inside one partition

Run:

```bash
docker exec kafka1 ls -lah /var/lib/kafka/data/m6-replication-topic-0
```

Example output:

```text
00000000000000000000.log
00000000000000000000.index
00000000000000000000.timeindex
partition.metadata
```

How to read it:

- `.log` contains the actual record data.
- `.index` maps offsets to positions in the log file.
- `.timeindex` helps find records by timestamp.
- `partition.metadata` stores metadata for that partition directory.

This is why Kafka is often described as a distributed commit log. At the storage level, Kafka is writing ordered log segments to disk.

---

## 16. Lab Cleanup

Make sure all brokers are running before continuing to later modules:

```bash
cd docker
docker compose start kafka1 kafka2 kafka3
docker compose ps
```

Expected state:

```text
kafka1   Up (healthy)
kafka2   Up (healthy)
kafka3   Up (healthy)
```

If a broker remains unhealthy, inspect its logs:

```bash
docker logs kafka2 --tail 100
```

---

## 17. Module 6 Summary

| Concept | Key point |
|---|---|
| Cluster membership | Brokers join the Kafka cluster and are tracked by metadata |
| KRaft controller | Kafka 4 uses an internal controller quorum, not ZooKeeper |
| Controller | Manages metadata, broker membership, and leader election |
| Partition | One ordered log inside a topic |
| Leader | Broker handling reads and writes for a partition |
| Replica | A copy of a partition on a broker |
| ISR | Replicas that are caught up and safe for reliable writes/failover |
| Broker failure | The controller elects new leaders from ISR |
| Request processing | Clients discover metadata, then talk to partition leaders |
| Physical storage | Topic partitions are stored as log segment files on broker disks |

---

## Module 6 Exit Criteria

A learner is ready for Module 7 when:

- [ ] They can describe a topic and identify partitions, leaders, replicas, and ISR
- [ ] They can explain what changes when one broker stops
- [ ] They can explain why Kafka continues working with one broker down in a 3-broker replicated cluster
- [ ] They can explain why producers and consumers talk to partition leaders
- [ ] They can inspect broker storage under `/var/lib/kafka/data`
- [ ] They can explain how Module 6 internals support Module 7 reliability settings such as `acks="all"` and `min.insync.replicas`

---

## Further Reading

- Kafka documentation - Replication
- Kafka documentation - KRaft mode
- Kafka documentation - Topic configuration
- Kafka documentation - Operations and storage
