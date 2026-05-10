# Module 7: Reliable Data Delivery

**Course:** Apache Kafka (2-Day, 14 Hours, Instructor-Led)
**Module duration:** ~75 minutes including hands-on validation
**Position in course:** Day 1, sixth session (after Module 6 - Kafka Internals)

---

## Learning Objectives

By the end of this module, learners will be able to:

1. Explain Kafka's reliability guarantees and where those guarantees stop
2. Configure producers for durable writes using `acks`, retries, and idempotence
3. Configure consumers for safe processing using manual commits and idempotent destinations
4. Explain how replication factor, ISR, and `min.insync.replicas` affect write safety
5. Validate reliability behavior by stopping brokers and observing topic state
6. Describe how to design applications for at-least-once delivery without creating duplicate downstream results

---

## 1. Reliability Is a System Property

Kafka reliability is not controlled by one setting. It is the result of several layers working together:

| Layer | Reliability responsibility |
|---|---|
| Broker/topic | Replication, ISR, retention, durable log storage |
| Producer | Acknowledgements, retries, idempotence, ordering |
| Consumer | Offset commits, processing order, replay behavior |
| Destination system | Idempotent writes, deduplication, transactions |
| Operations | Monitoring, alerting, backup, failure testing |

This is why Module 6 matters. Reliable delivery depends on internals such as:

- Partition leaders
- Replicas
- ISR
- Broker failure detection
- Leader election
- Durable log storage

If a producer writes to a replicated topic but uses weak acknowledgement settings, data can still be lost. If a consumer reads safely but writes duplicates to the destination, the final result can still be wrong.

A reliable Kafka system is designed end to end.

---

## 2. Kafka Delivery Guarantees

Kafka applications usually discuss three delivery guarantees.

| Guarantee | Meaning | Typical cause |
|---|---|---|
| At-most-once | A record may be processed zero or one time | Commit before processing |
| At-least-once | A record is processed one or more times | Process first, commit after |
| Exactly-once | A record affects the final result once | Requires idempotence or transactions |

### 2.1 At-Most-Once

At-most-once means records are never processed twice, but they can be lost.

Example flow:

1. Consumer receives records.
2. Consumer commits offsets.
3. Consumer starts processing.
4. Application crashes before processing finishes.

After restart, Kafka resumes after the committed offset, so those records are skipped.

At-most-once is acceptable only when occasional loss is acceptable, such as some metrics or debug logs.

### 2.2 At-Least-Once

At-least-once means records are not lost, but they may be processed more than once.

Example flow:

1. Consumer receives records.
2. Consumer writes them to a destination.
3. Application crashes before committing offsets.
4. After restart, Kafka redelivers the same records.

This is the most common Kafka application model. It is safe when the destination write is **idempotent**.

### 2.3 Exactly-Once

Exactly-once means each input record affects the final result once.

Kafka supports exactly-once patterns with:

- Idempotent producers
- Transactions
- Kafka Streams exactly-once processing

However, exactly-once is not automatic for every external database, API, or search index. If Kafka writes to Elasticsearch, MySQL, or a REST API, the destination must also participate in the design.

For many real systems, the practical design is:

```text
Kafka at-least-once delivery + idempotent destination writes
```

---

## 3. Broker-Side Reliability

Kafka stores data in topic partitions. Reliability starts with topic configuration.

### 3.1 Replication Factor

Replication factor controls how many copies of each partition exist.

In this course, topics are created with replication factor 3:

```text
Partition 0 replicas: broker 1, broker 2, broker 3
```

With replication factor 3, Kafka can lose one broker and still have copies available on two other brokers.

### 3.2 ISR

ISR means **in-sync replicas**. These are replicas that are caught up enough to be considered safe.

Describe a topic:

```bash
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:29092 \
  --describe \
  --topic m7-reliable-topic
```

Example output:

```text
Topic: m7-reliable-topic  PartitionCount: 3  ReplicationFactor: 3
  Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
  Partition: 1  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
  Partition: 2  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2
```

How to read it:

- `Replicas` shows where all copies should exist.
- `Isr` shows which copies are currently in sync.
- Reliable writes depend on ISR, not only on replica assignment.

### 3.3 min.insync.replicas

`min.insync.replicas` sets the minimum number of in-sync replicas required for a successful highly reliable write.

In this course platform:

```yaml
KAFKA_MIN_INSYNC_REPLICAS: 2
```

With a 3-broker cluster:

- If all 3 brokers are healthy, ISR is usually 3.
- If 1 broker is down, ISR can still be 2.
- With `acks="all"`, reliable writes can still succeed when ISR is at least 2.

This setting only protects producers that use `acks="all"`. If the producer uses `acks=1`, the producer only waits for the leader.

---

## 4. Using Producers in a Reliable System

A reliable producer waits for strong acknowledgement, retries transient failures, and avoids duplicate writes caused by retries.

### 4.1 Acknowledgements

`acks` controls when the broker responds to the producer.

| Setting | Meaning | Reliability |
|---|---|---|
| `acks=0` | Producer does not wait for broker acknowledgement | Lowest |
| `acks=1` | Leader writes the record and acknowledges | Medium |
| `acks="all"` | Leader waits for in-sync replicas | Highest |

For reliable systems, use:

```python
acks="all"
```

This works with broker-side `min.insync.replicas`.

### 4.2 Retries

Retries handle temporary errors:

- Leader election in progress
- Temporary network issue
- Broker unavailable briefly
- Metadata refresh needed

Example:

```python
retries=10
retry_backoff_ms=300
```

Retries improve reliability, but they can also cause duplicates if a send actually succeeded and the acknowledgement was lost.

### 4.3 Ordering and max_in_flight

If multiple requests are in flight and one request is retried, later records may be acknowledged before earlier records. That can affect ordering.

For strict ordering with `kafka-python`, use:

```python
max_in_flight_requests_per_connection=1
```

This is slower, but easier to reason about.

### 4.4 Idempotent Producer

An idempotent producer lets Kafka deduplicate retried sends within a producer session.

```python
enable_idempotence=True
```

With idempotence:

- Kafka assigns a producer id.
- Records include sequence numbers.
- Retried duplicates can be detected by the broker.
- The same record is not appended twice because of a retry.

In `kafka-python`, keep:

```python
max_in_flight_requests_per_connection=1
```

This is stricter than the Java client, but it matches the library used in this course.

### 4.5 Reliable Producer Configuration

Recommended teaching configuration:

```python
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092", "localhost:9093", "localhost:9094"],
    key_serializer=lambda key: key.encode("utf-8"),
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    acks="all",
    retries=10,
    retry_backoff_ms=300,
    enable_idempotence=True,
    max_in_flight_requests_per_connection=1,
)
```

What each setting contributes:

| Setting | Why it matters |
|---|---|
| `acks="all"` | Waits for in-sync replicas |
| `retries=10` | Handles transient failures |
| `retry_backoff_ms=300` | Avoids tight retry loops |
| `enable_idempotence=True` | Prevents duplicate appends from producer retries |
| `max_in_flight_requests_per_connection=1` | Preserves ordering in `kafka-python` |

---

## 5. Using Consumers in a Reliable System

Producer reliability protects writes into Kafka. Consumer reliability protects processing after Kafka.

The key rule:

> Process first, commit offsets after successful processing.

### 5.1 Consumer Offsets

Kafka stores consumer progress as offsets.

If a consumer group has committed offset 10 for a partition, the next read starts at offset 10. This means offsets represent the next record to consume, not the last processed record.

### 5.2 Auto-Commit

Auto-commit is simple:

```python
enable_auto_commit=True
```

The consumer periodically commits its current position in the background.

This is convenient for learning, but it is not ideal for reliable processing because records may be committed before the application has fully processed them.

### 5.3 Manual Commit After Processing

For reliable consumers:

```python
consumer = KafkaConsumer(
    "m7-reliable-topic",
    bootstrap_servers=["localhost:9092", "localhost:9093", "localhost:9094"],
    group_id="m7-reliable-consumer",
    enable_auto_commit=False,
    auto_offset_reset="earliest",
)
```

Processing pattern:

```python
records = consumer.poll(timeout_ms=1000)

for topic_partition, messages in records.items():
    for message in messages:
        process(message)

consumer.commit()
```

If `process(message)` succeeds and the application crashes before `commit()`, Kafka redelivers the message. That creates at-least-once delivery.

### 5.4 Idempotent Destination Writes

At-least-once delivery means duplicate processing is possible. The destination must handle it safely.

Examples of idempotent writes:

| Destination | Idempotent strategy |
|---|---|
| Elasticsearch | Use stable document id |
| Relational database | Use `INSERT ... ON DUPLICATE KEY UPDATE` / upsert |
| Key-value store | Write by stable key |
| Object storage | Use deterministic object path |

Example:

```text
Kafka key: customer-42
Elasticsearch document id: customer-42
```

If the same message is processed twice, it updates the same document instead of creating duplicates.

### 5.5 Bad Records and DLQs

Reliable systems also need a plan for bad records.

A bad record might:

- Fail deserialization
- Miss a required field
- Contain an invalid value
- Be rejected by the destination

Common strategy:

```text
input topic -> consumer -> destination
                    |
                    +-> dead letter topic
```

A dead letter queue (DLQ) should include:

- Original topic
- Original partition
- Original offset
- Original key and value
- Error message
- Timestamp

This lets the pipeline continue while bad records are investigated.

---

## 6. Application Reliability Scripts

This module includes two small Python scripts that make the producer and consumer reliability patterns concrete.

```text
python_scripts/module07/
  01_reliable_producer.py
  02_reliable_consumer.py
```

These scripts do not stop or start Docker containers. Broker failure testing stays in the CLI lab later in this module, where the behavior is easier to see and safer to control.

### 6.1 Reliable Producer Script

Run from the repository root:

```bash
source .venv/bin/activate
python python_scripts/module07/01_reliable_producer.py --count 8
```

The script writes keyed JSON records to:

```text
m7-reliable-topic
```

It uses the reliable producer settings from this module:

```python
acks="all"
retries=10
retry_backoff_ms=300
enable_idempotence=True
max_in_flight_requests_per_connection=1
```

Expected output:

```text
OK  key=ORD-1000 event_id=m7-0000 partition=1 offset=42
OK  key=ORD-1001 event_id=m7-0001 partition=0 offset=37
OK  key=ORD-1002 event_id=m7-0002 partition=2 offset=39
```

How to read it:

- `key` is the order id used for partition routing.
- `event_id` identifies the event.
- `partition` shows where Kafka stored the record.
- `offset` confirms the record's position inside that partition.
- An `OK` line means Kafka acknowledged the write using the reliable config.

### 6.2 Reliable Consumer Script

Run:

```bash
python python_scripts/module07/02_reliable_consumer.py --max-messages 8
```

The consumer uses:

```python
enable_auto_commit=False
```

It processes records first, then commits offsets manually.

Expected output:

```text
UPSERT sink[ORD-1000] partition=1 offset=42 event_id=m7-0000 order_id=ORD-1000 status=created
UPSERT sink[ORD-1001] partition=0 offset=37 event_id=m7-0001 order_id=ORD-1001 status=paid
Committed offsets after processing 5 record(s).
```

How to read it:

- `UPSERT sink[...]` simulates an idempotent destination write.
- The sink key is stable, so duplicate processing overwrites the same logical record.
- Offsets are committed only after a batch has been processed.

### 6.3 Re-Delivery Demonstration

To demonstrate at-least-once behavior, use a fresh group id and simulate a crash before commit.

First produce a few records:

```bash
python python_scripts/module07/01_reliable_producer.py --count 4
```

Then consume with a simulated failure:

```bash
python python_scripts/module07/02_reliable_consumer.py \
  --group-id m7-fail-demo \
  --fail-after 2
```

Expected behavior:

```text
UPSERT sink[ORD-1000] ...
UPSERT sink[ORD-1001] ...

Simulated crash now: processed records are NOT committed.
Run the same group again to observe re-delivery.
```

Run the same group again:

```bash
python python_scripts/module07/02_reliable_consumer.py \
  --group-id m7-fail-demo \
  --max-messages 4
```

The records processed before the simulated crash are delivered again because their offsets were not committed. This is at-least-once delivery in action.

The important design lesson is not to avoid re-delivery entirely. The important design lesson is to make processing safe when re-delivery happens.

---

## 7. Validating System Reliability

Reliability should be tested. It should not only be assumed from configuration.

This section uses the existing Docker cluster and the topic:

```text
m7-reliable-topic
```

### 7.1 Confirm Replication

Run:

```bash
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:29092 \
  --describe \
  --topic m7-reliable-topic
```

Expected output:

```text
Topic: m7-reliable-topic  PartitionCount: 3  ReplicationFactor: 3
  Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
  Partition: 1  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
  Partition: 2  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2
```

What to check:

- `ReplicationFactor` is 3.
- Each partition has three replicas.
- `Isr` contains three brokers before failure.

### 7.2 Stop One Broker

From the `docker/` directory:

```bash
docker compose stop kafka2
```

Describe the topic again:

```bash
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:29092 \
  --describe \
  --topic m7-reliable-topic
```

Expected pattern:

```text
Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,3
Partition: 1  Leader: 3  Replicas: 2,3,1  Isr: 3,1
Partition: 2  Leader: 3  Replicas: 3,1,2  Isr: 3,1
```

How to read it:

- Broker `2` leaves ISR.
- If broker `2` was a leader, another broker becomes leader.
- The topic still has two in-sync replicas.

With `min.insync.replicas=2`, reliable writes can still succeed.

### 7.3 Produce While One Broker Is Down

Run:

```bash
docker exec -it kafka1 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka1:29092 \
  --topic m7-reliable-topic \
  --producer-property acks=all
```

Type:

```text
reliable message while kafka2 is down
```

Press `Ctrl+C`.

Why it works:

- The producer uses `acks=all`.
- Two replicas are still in ISR.
- The broker requires at least two in-sync replicas.
- The write can still be acknowledged safely.

### 7.4 Consume the Record

Run:

```bash
docker exec -it kafka1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka1:29092 \
  --topic m7-reliable-topic \
  --from-beginning
```

Expected output includes:

```text
reliable message while kafka2 is down
```

This confirms the cluster accepted and stored the message while one broker was unavailable.

Press `Ctrl+C` to stop.

### 7.5 Restart the Broker

From the `docker/` directory:

```bash
docker compose start kafka2
```

Wait 15-30 seconds, then describe the topic:

```bash
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:29092 \
  --describe \
  --topic m7-reliable-topic
```

Expected pattern:

```text
Isr: 1,3,2
```

The exact order may differ. The important point is that broker `2` returns to ISR after catching up.

---

## 8. Optional Validation: Force a Write Failure Safely

Do not stop two brokers in this course cluster just to force a write failure. Because all three brokers are also KRaft controllers, stopping two brokers can remove controller quorum and make the cluster unavailable.

A safer demonstration is to create a stricter topic that requires all three replicas to be in sync.

### 8.1 Create a Strict Topic

Run:

```bash
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:29092 \
  --create \
  --if-not-exists \
  --topic m7-strict-topic \
  --partitions 3 \
  --replication-factor 3 \
  --config min.insync.replicas=3
```

Describe it:

```bash
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:29092 \
  --describe \
  --topic m7-strict-topic
```

Expected output includes:

```text
Configs: min.insync.replicas=3
```

This topic requires all three replicas to be in sync for `acks=all` writes.

### 8.2 Stop One Broker

From the `docker/` directory:

```bash
docker compose stop kafka2
```

Now ISR is only two brokers, but the topic requires three.

### 8.3 Try to Produce with acks=all

Run:

```bash
docker exec -it kafka1 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka1:29092 \
  --topic m7-strict-topic \
  --producer-property acks=all \
  --producer-property retries=0
```

Type:

```text
this write should fail
```

Expected behavior:

- The producer should report an error such as `NotEnoughReplicas` or `NotEnoughReplicasAfterAppend`.
- The exact message can vary by Kafka version and timing.

What this proves:

- `acks=all` respects ISR safety.
- `min.insync.replicas=3` prevents writes when fewer than three replicas are in sync.
- Kafka can prefer rejecting writes over accepting unsafe writes.

From the `docker/` directory, restart the broker:

```bash
docker compose start kafka2
```

Wait for ISR recovery before continuing.

---

## 9. Reliability Design Checklist

Use this checklist when designing a reliable Kafka pipeline.

### Producer checklist

- Use `acks="all"` for important data
- Use retries with backoff
- Use idempotence where supported
- Preserve ordering when required
- Use stable keys for entity ordering
- Monitor producer errors and retry exhaustion

### Topic checklist

- Use replication factor 3 for important topics
- Set `min.insync.replicas=2` for a 3-broker cluster
- Avoid relying on auto-created topics
- Confirm partitions, leaders, replicas, and ISR
- Set retention based on replay requirements

### Consumer checklist

- Disable auto-commit for critical processing
- Process records before committing offsets
- Make destination writes idempotent
- Handle poison messages
- Use DLQs for records that cannot be processed
- Monitor consumer lag

### Validation checklist

- Stop one broker and confirm writes still work
- Confirm ISR shrinks and recovers
- Confirm consumers can replay records
- Test duplicate processing behavior
- Test destination idempotency

---

## 10. Common Reliability Mistakes

| Mistake | Consequence |
|---|---|
| Using `acks=1` for critical data | Leader failure can lose acknowledged records |
| Replication factor 1 | Broker loss means partition loss |
| Committing before processing | Records can be skipped after crash |
| Non-idempotent destination writes | Duplicate processing creates duplicate results |
| No DLQ strategy | One bad record can block progress |
| No replay plan | Recovery becomes manual and risky |
| Never testing broker failure | Reliability assumptions remain unproven |

---

## 11. Module 7 Summary

| Concept | Key point |
|---|---|
| Reliability | A system property across broker, producer, consumer, and destination |
| At-most-once | No duplicates, but possible data loss |
| At-least-once | No loss, but possible duplicate processing |
| Exactly-once | Final result is affected once; requires stronger design |
| Replication factor | Number of copies of each partition |
| ISR | Replicas caught up enough for safe writes and failover |
| `min.insync.replicas` | Minimum ISR count required for reliable writes |
| `acks="all"` | Producer waits for in-sync replica acknowledgement |
| Idempotent producer | Prevents duplicate appends caused by retries |
| Manual commit | Lets consumers commit only after successful processing |
| Idempotent sink | Makes duplicate processing safe |
| DLQ | Stores failed records for investigation without blocking the pipeline |

---

## Module 7 Exit Criteria

A learner is ready for Module 8 when:

- [ ] They can explain `acks=1` vs `acks="all"`
- [ ] They can explain how `min.insync.replicas` works with ISR
- [ ] They can describe why retries can create duplicates without idempotence
- [ ] They can explain why consumers should commit after processing
- [ ] They can describe at-most-once, at-least-once, and exactly-once delivery
- [ ] They can stop one broker and explain the ISR changes
- [ ] They can explain why idempotent destination writes are needed for at-least-once systems

---

## Further Reading

- Kafka documentation - Message delivery semantics
- Kafka documentation - Producer configuration
- Kafka documentation - Consumer configuration
- Kafka documentation - Replication
- Kafka documentation - Exactly-once semantics
