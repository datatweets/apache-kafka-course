# Module 11: Administering Kafka

**Course:** Apache Kafka (2-Day, 14 Hours, Instructor-Led)
**Module duration:** ~75 minutes, hands-on CLI focused
**Position in course:** Day 2, administration session

---

## Learning Objectives

By the end of this module, learners will be able to:

1. Use Kafka CLI tools to inspect and manage topics
2. Produce and consume records from the command line
3. Inspect consumer groups and understand lag
4. Reset consumer group offsets safely
5. Change topic-level dynamic configurations
6. Remove dynamic config overrides after testing

This module uses Kafka CLI tools inside the Docker broker container. No Python code is required.

---

## 1. Admin Command Pattern

Run Kafka CLI commands inside `kafka1`:

```bash
docker exec kafka1 /opt/kafka/bin/<tool-name>.sh \
  --bootstrap-server kafka1:29092 \
  ...
```

Use `kafka1:29092` because the command runs inside Docker. Do not use `localhost:9092` inside `docker exec`.

Main tools in this module:

| Tool | Purpose |
|---|---|
| `kafka-topics.sh` | Create, list, describe, alter topics |
| `kafka-console-producer.sh` | Produce records manually |
| `kafka-console-consumer.sh` | Consume records manually |
| `kafka-consumer-groups.sh` | Inspect and reset consumer groups |
| `kafka-configs.sh` | Inspect and change dynamic configs |

---

## 2. Topic Operations

Kafka topics are the main administrative unit. Admins need to create, inspect, and alter them safely.

This course already creates:

```text
m11-admin-topic
m11-dynamic-config-topic
```

### 2.1 List Topics

```bash
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:29092 \
  --list
```

Expected output includes:

```text
m11-admin-topic
m11-dynamic-config-topic
```

What it means:

- The broker is reachable.
- Metadata can be read.
- Course topics have been created.

### 2.2 Describe a Topic

```bash
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:29092 \
  --describe \
  --topic m11-admin-topic
```

Example output:

```text
Topic: m11-admin-topic  PartitionCount: 3  ReplicationFactor: 3
  Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
  Partition: 1  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
  Partition: 2  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2
```

Read these fields:

- `PartitionCount`: number of partitions
- `ReplicationFactor`: number of copies per partition
- `Leader`: broker handling reads/writes for that partition
- `Replicas`: brokers assigned to store copies
- `Isr`: replicas currently in sync

### 2.3 Create a Topic

```bash
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:29092 \
  --create \
  --if-not-exists \
  --topic m11-created-topic \
  --partitions 3 \
  --replication-factor 3
```

Expected output:

```text
Created topic m11-created-topic.
```

Use `--if-not-exists` in labs so re-running the command is safe.

### 2.4 Add Partitions

Increase `m11-created-topic` from 3 to 6 partitions:

```bash
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:29092 \
  --alter \
  --topic m11-created-topic \
  --partitions 6
```

Verify:

```bash
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:29092 \
  --describe \
  --topic m11-created-topic
```

Expected output includes:

```text
PartitionCount: 6
```

Important:

- You can increase partition count.
- You cannot reduce partition count.
- Adding partitions can change key-to-partition routing for future records.

---

## 3. Consuming and Producing

Console producer and consumer are useful for quick validation and debugging.

### 3.1 Produce Records

```bash
docker exec -it kafka1 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka1:29092 \
  --topic m11-admin-topic
```

Type:

```text
admin test 1
admin test 2
admin test 3
```

Press `Ctrl+C` to exit.

### 3.2 Consume From Beginning

```bash
docker exec -it kafka1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka1:29092 \
  --topic m11-admin-topic \
  --from-beginning \
  --max-messages 3
```

Expected output:

```text
admin test 1
admin test 2
admin test 3
```

`--from-beginning` starts from the earliest retained offset when the consumer group has no committed offset.

### 3.3 Produce With Keys

Kafka keys are separated from values in the console producer with a parser setting.

```bash
docker exec -it kafka1 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka1:29092 \
  --topic m11-admin-topic \
  --property parse.key=true \
  --property key.separator=:
```

Type:

```text
user-1:login
user-1:checkout
user-2:login
```

Press `Ctrl+C`.

Keys are useful because records with the same key go to the same partition.

### 3.4 Consume With Keys

```bash
docker exec -it kafka1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka1:29092 \
  --topic m11-admin-topic \
  --from-beginning \
  --property print.key=true \
  --property key.separator=" : " \
  --max-messages 6
```

Expected output includes both unkeyed and keyed records. Unkeyed records may show `null` as the key.

---

## 4. Consumer Groups

A consumer group tracks offsets per topic partition. Admins inspect groups to understand lag and processing state.

### 4.1 Start a Named Consumer Group

Run this and keep it open:

```bash
docker exec -it kafka1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka1:29092 \
  --topic m11-admin-topic \
  --group m11-admin-group
```

In another terminal, produce records:

```bash
docker exec -it kafka1 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka1:29092 \
  --topic m11-admin-topic
```

Type:

```text
group message 1
group message 2
group message 3
```

The consumer should print the messages.

### 4.2 List Consumer Groups

```bash
docker exec kafka1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka1:29092 \
  --list
```

Expected output includes:

```text
m11-admin-group
```

### 4.3 Describe Consumer Group Offsets

Stop the console consumer with `Ctrl+C`, then run:

```bash
docker exec kafka1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka1:29092 \
  --describe \
  --group m11-admin-group
```

Example output:

```text
GROUP            TOPIC             PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
m11-admin-group  m11-admin-topic   0          4               4               0
m11-admin-group  m11-admin-topic   1          3               3               0
m11-admin-group  m11-admin-topic   2          2               2               0
```

How to read it:

- `CURRENT-OFFSET`: next offset the group will read
- `LOG-END-OFFSET`: next offset available in the topic
- `LAG`: records not yet consumed by this group

If `LAG` is `0`, the group has caught up.

### 4.4 Create Lag

Produce more records while the consumer is stopped:

```bash
docker exec -it kafka1 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka1:29092 \
  --topic m11-admin-topic
```

Type:

```text
lag message 1
lag message 2
lag message 3
```

Press `Ctrl+C`.

Describe the group again:

```bash
docker exec kafka1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka1:29092 \
  --describe \
  --group m11-admin-group
```

Expected result:

```text
LAG
3
```

The exact distribution across partitions may differ.

---

## 5. Reset Consumer Group Offsets

Offset reset is powerful and risky. Only reset offsets when the consumer group is stopped.

### 5.1 Dry Run Reset To Earliest

```bash
docker exec kafka1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka1:29092 \
  --group m11-admin-group \
  --topic m11-admin-topic \
  --reset-offsets \
  --to-earliest \
  --dry-run
```

Expected output shows what would change, but does not apply it.

Look for columns like:

```text
CURRENT-OFFSET  LOG-END-OFFSET  NEW-OFFSET
```

### 5.2 Execute Reset To Earliest

```bash
docker exec kafka1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka1:29092 \
  --group m11-admin-group \
  --topic m11-admin-topic \
  --reset-offsets \
  --to-earliest \
  --execute
```

This makes the group read from the earliest retained offsets next time it starts.

### 5.3 Reset To Latest

Use this to skip existing backlog:

```bash
docker exec kafka1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka1:29092 \
  --group m11-admin-group \
  --topic m11-admin-topic \
  --reset-offsets \
  --to-latest \
  --execute
```

Use with care. Skipping backlog means the group will not process older records.

---

## 6. Dynamic Configuration Changes

Kafka supports dynamic configuration changes without editing broker files or restarting the cluster.

This section uses:

```text
m11-dynamic-config-topic
```

### 6.1 Describe Topic Configs

```bash
docker exec kafka1 /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka1:29092 \
  --entity-type topics \
  --entity-name m11-dynamic-config-topic \
  --describe
```

If there are no overrides, output may be short or empty. That means the topic is using broker defaults.

### 6.2 Change Retention

Set topic retention to 10 minutes:

```bash
docker exec kafka1 /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka1:29092 \
  --entity-type topics \
  --entity-name m11-dynamic-config-topic \
  --alter \
  --add-config retention.ms=600000
```

Expected output:

```text
Completed updating config for topic m11-dynamic-config-topic.
```

Verify:

```bash
docker exec kafka1 /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka1:29092 \
  --entity-type topics \
  --entity-name m11-dynamic-config-topic \
  --describe
```

Expected output includes:

```text
retention.ms=600000
```

### 6.3 Change Max Message Size

Set topic-level max message size:

```bash
docker exec kafka1 /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka1:29092 \
  --entity-type topics \
  --entity-name m11-dynamic-config-topic \
  --alter \
  --add-config max.message.bytes=1048576
```

Verify:

```bash
docker exec kafka1 /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka1:29092 \
  --entity-type topics \
  --entity-name m11-dynamic-config-topic \
  --describe
```

Expected output includes:

```text
max.message.bytes=1048576
```

### 6.4 Remove Config Overrides

Remove the overrides after the lab:

```bash
docker exec kafka1 /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka1:29092 \
  --entity-type topics \
  --entity-name m11-dynamic-config-topic \
  --alter \
  --delete-config retention.ms,max.message.bytes
```

Verify:

```bash
docker exec kafka1 /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka1:29092 \
  --entity-type topics \
  --entity-name m11-dynamic-config-topic \
  --describe
```

The deleted configs should no longer appear.

---

## 7. Useful Admin Checks

### Topic Metadata

```bash
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:29092 \
  --describe \
  --topic m11-admin-topic
```

Use this to inspect leaders, replicas, and ISR.

### Consumer Group Lag

```bash
docker exec kafka1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka1:29092 \
  --describe \
  --group m11-admin-group
```

Use this to check whether consumers are caught up.

### Topic Configs

```bash
docker exec kafka1 /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka1:29092 \
  --entity-type topics \
  --entity-name m11-dynamic-config-topic \
  --describe
```

Use this to inspect dynamic overrides.

---

## 8. Admin Safety Rules

| Operation | Safety note |
|---|---|
| List topics | Safe |
| Describe topics | Safe |
| Produce test records | Safe on lab topics |
| Consume from beginning | Safe with a new group id |
| Add partitions | Cannot be undone; may change key routing |
| Reset offsets | Only when group is stopped; can replay or skip data |
| Change retention | Can delete data sooner than expected |
| Change max message size | Can affect producers and consumers |
| Delete topics | Avoid in this module unless explicitly needed |

---

## 9. Module 11 Summary

| Concept | Key point |
|---|---|
| Topic operations | Use `kafka-topics.sh` to list, create, describe, and alter topics |
| Partitions | Control parallelism; can be increased but not decreased |
| Replication | Topic describe shows replicas and ISR |
| Console producer | Quick way to write test records |
| Console consumer | Quick way to read and debug records |
| Consumer group | Tracks committed offsets per topic partition |
| Lag | Difference between log end offset and current group offset |
| Offset reset | Replays or skips data for a stopped group |
| Dynamic configs | Topic settings can be changed with `kafka-configs.sh` |

---

## Module 11 Exit Criteria

A learner completes this module when:

- [ ] They can list and describe topics
- [ ] They can create a topic with partitions and replication factor
- [ ] They can increase a topic's partition count
- [ ] They can produce and consume records with CLI tools
- [ ] They can list and describe consumer groups
- [ ] They can explain `CURRENT-OFFSET`, `LOG-END-OFFSET`, and `LAG`
- [ ] They can dry-run and execute a consumer group offset reset
- [ ] They can add, verify, and remove topic-level dynamic configs

---

## Further Reading

- Kafka documentation - Topic operations
- Kafka documentation - Consumer group command
- Kafka documentation - Dynamic broker and topic configs
- Kafka documentation - Console producer and console consumer
