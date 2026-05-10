# Module 8: Designing Kafka Data Pipelines

**Course:** Apache Kafka (2-Day, 14 Hours, Instructor-Led)
**Module duration:** ~60 minutes, design-focused
**Position in course:** Day 2, first pipeline architecture session

---

## Learning Objectives

By the end of this module, learners will be able to:

1. Identify the main requirements that shape a Kafka data pipeline
2. Compare batch, micro-batch, and real-time pipeline expectations
3. Explain how reliability, throughput, failure handling, and data format choices affect pipeline design
4. Decide when to use Kafka Connect and when to write a custom producer or consumer
5. Design a clear event contract using keys, envelopes, timestamps, metadata, idempotency, and versioning

This module is intentionally architecture-focused. Module 9 turns these design decisions into a working MySQL -> Kafka -> Elasticsearch pipeline.

---

## 1. What Is a Kafka Data Pipeline?

A **data pipeline** moves data from one or more source systems to one or more destination systems. Kafka sits in the middle as a durable, replayable event log.

```
Source System  ->  Kafka Topic(s)  ->  Destination System(s)
 database          durable log         search index
 application       partitions          analytics platform
 file/API          retention           microservice
```

Kafka does not magically make a pipeline correct. It provides the transport, storage, ordering per partition, replication, replay, and consumer group model. The pipeline design still has to answer important questions:

- How quickly must data arrive?
- What happens if a system fails?
- Can records be processed more than once?
- How much throughput is expected?
- What format should events use?
- Where should transformations happen?
- How tightly should producers and consumers depend on each other?

Poor pipeline design often shows up later as duplicate records, missing updates, slow consumers, inconsistent schemas, or systems that are difficult to change. Good design makes those trade-offs explicit before implementation starts.

---

## 2. Pipeline Requirements

Before choosing tools or writing code, describe the pipeline's requirements. The same Kafka cluster can support many different pipeline styles, but the correct design depends on the business need.

### 2.1 Timeliness

Timeliness describes how quickly data must move from source to destination.

| Style | Typical delay | Description | Example |
|---|---:|---|---|
| Batch | Minutes to hours | Data is moved in large scheduled chunks | Daily sales export |
| Micro-batch | Seconds to minutes | Small batches are moved frequently | Poll database every 30 seconds |
| Real time | Milliseconds to seconds | Events are emitted as soon as they happen | Fraud detection, user activity tracking |

Kafka is usually chosen for micro-batch or real-time systems, but not every Kafka pipeline needs millisecond latency. A reporting pipeline that updates every minute may be simpler and more reliable than a fully real-time design.

Important design questions:

- Is the destination expected to be current immediately?
- Is a delay of 5 seconds acceptable? 1 minute? 15 minutes?
- Does the source system produce events naturally, or must it be polled?
- Is low latency more important than throughput and cost?

Timeliness affects producer batching, consumer polling, connector settings, monitoring thresholds, and alerting.

### 2.2 Reliability

Reliability defines what the pipeline promises when failures happen. Kafka systems normally discuss three delivery semantics:

| Guarantee | Meaning | Risk |
|---|---|---|
| At-most-once | A record is processed zero or one time | Data may be lost |
| At-least-once | A record is processed one or more times | Duplicates may occur |
| Exactly-once | A record affects the final result once | Requires stricter design |

Most Kafka pipelines are designed around **at-least-once delivery** plus **idempotent writes**. This is practical and reliable: Kafka may redeliver a record after a crash, but the destination can safely apply the same record again without creating duplicates.

For example, writing to Elasticsearch with a stable document id is idempotent:

```
PUT /customers/_doc/42
```

If the same customer event is processed twice, document `42` is updated twice to the same final state instead of creating two separate documents.

Important design questions:

- Can the same event be safely processed twice?
- What unique key identifies the record in the destination?
- Should offsets be committed before or after processing?
- What happens if the destination write succeeds but the consumer crashes before committing the offset?

### 2.3 Throughput

Throughput is the amount of data the pipeline can move per second. It is shaped by producers, brokers, topics, consumers, and destination systems.

Common throughput controls:

| Area | Design choice | Effect |
|---|---|---|
| Topic partitions | More partitions | More parallelism, more operational overhead |
| Producer batching | Larger batches / higher `linger_ms` | Better throughput, slightly higher latency |
| Compression | `lz4`, `gzip`, `zstd`, etc. | Less network/disk usage, more CPU |
| Consumer groups | More consumers | More parallel processing, limited by partition count |
| Destination writes | Bulk/index batches | Higher sink throughput, more complex retries |

Kafka can handle high throughput, but the slowest part of the pipeline still controls end-to-end speed. Often the bottleneck is not Kafka itself; it is the source database, the sink API, Elasticsearch indexing, network bandwidth, or transformation logic.

### 2.4 Backpressure

Backpressure happens when one part of the pipeline produces data faster than another part can process it.

In Kafka, backpressure commonly appears as **consumer lag**:

```
Produced offset: 100000
Consumed offset:  76000
Lag:              24000 records
```

Lag is not always bad. Kafka is designed to buffer data. Short-term lag during a spike is normal. Persistent lag means the consumer side cannot keep up.

Design responses include:

- Add partitions and consumers
- Increase consumer batch size
- Use bulk writes to the destination
- Reduce transformation cost
- Add retries with backoff instead of tight retry loops
- Slow down the producer if the destination cannot scale

The important point: Kafka absorbs pressure, but it does not remove the need to size the entire pipeline.

---

## 3. Data Formats and Schemas

Kafka stores bytes. The meaning of those bytes is defined by the producer and consumer.

### 3.1 JSON

JSON is simple and readable, so it is useful for teaching, demos, debugging, and early-stage systems.

Example:

```json
{
  "customer_id": "C001",
  "name": "Alice Tan",
  "country": "Singapore"
}
```

Benefits:

- Easy to inspect in Kafdrop or console consumers
- Native support in most languages
- Good for learning and small internal pipelines

Trade-offs:

- Larger payloads than binary formats
- No built-in schema enforcement
- Field types can drift over time
- Consumers may fail if producers change structure unexpectedly

### 3.2 Avro, Protobuf, and Schema Registry

Production Kafka pipelines often use Avro, Protobuf, or JSON Schema with a Schema Registry.

These formats help teams manage change:

- Producers publish events using a registered schema
- Consumers know what structure to expect
- Compatibility rules prevent breaking changes
- Schemas become part of the pipeline contract

This course uses JSON for readability, but the design principle still applies: every pipeline needs a clear event contract. In production, that contract should be enforced by tooling, not only by documentation.

### 3.3 Schema Evolution

Pipelines live longer than their first version. Event structures will change.

Common safe changes:

- Add an optional field
- Add a field with a default value
- Stop using a field but keep it present for older consumers

Risky changes:

- Rename a field without a compatibility plan
- Change a field type, such as string to number
- Remove a field that consumers still depend on
- Change the meaning of an existing field

Good event design assumes that multiple producer and consumer versions may run at the same time during deployments.

---

## 4. Transformations

A transformation changes data as it moves through the pipeline. Examples include filtering, enrichment, masking, aggregation, and reshaping.

There are three common places to transform data.

### 4.1 Source-Side Transformations

The source application transforms the data before sending it to Kafka.

```
Source -> transform -> Kafka
```

Benefits:

- Kafka receives clean, ready-to-use events
- Downstream consumers are simpler
- Sensitive fields can be removed early

Trade-offs:

- Raw source data may be lost
- Producer becomes responsible for downstream needs
- Changing transformation logic may require producer deployment

Use source-side transformation when the transformation is part of the event's meaning, such as converting an internal database row into a public `CustomerCreated` event.

### 4.2 Stream Processor Transformations

A separate stream processing application reads from one topic, transforms records, and writes to another topic.

```
Raw topic -> stream processor -> curated topic
```

Benefits:

- Raw data remains available for replay
- Transformation logic is isolated
- Multiple consumers can reuse curated topics

Trade-offs:

- More services to deploy and monitor
- More topics to manage
- More end-to-end latency

Use this when transformations are significant, shared, stateful, or likely to evolve independently.

### 4.3 Sink-Side Transformations

The consumer or sink connector transforms data just before writing to the destination.

```
Kafka -> sink transform -> destination
```

Benefits:

- Transformation can match destination-specific requirements
- Kafka topic remains general-purpose
- Useful for formatting documents, index names, or database writes

Trade-offs:

- Each sink may duplicate similar transformation logic
- Replaying to a new destination may require rewriting logic
- Bugs may be hidden inside sink-specific code

Use this for destination-specific formatting, not for core business meaning.

---

## 5. Failure Handling

Every real pipeline fails eventually. Sources go down, brokers restart, destinations reject records, schemas change, networks drop, and bad data appears.

A good design defines failure behavior before failures happen.

### 5.1 Retries

Retries are useful for temporary failures:

- Network timeout
- Destination service unavailable
- Leader election in Kafka
- Temporary rate limit

Retries should usually include **backoff**. Retrying immediately in a tight loop can make an overloaded system worse.

Design questions:

- How many times should the pipeline retry?
- Should retry delay increase over time?
- Which errors are retryable?
- Which errors should fail fast?

### 5.2 Dead Letter Queue

A **dead letter queue** (DLQ) is a Kafka topic where failed records are sent for later inspection.

```
input-topic -> consumer -> destination
                 |
                 +-> dlq-topic
```

DLQs are useful for records that are valid Kafka messages but cannot be processed:

- Missing required field
- Invalid enum value
- Destination rejects the record
- Deserialisation succeeds, but business validation fails

A DLQ record should include enough context to debug the failure:

- Original topic, partition, offset
- Original key and value
- Error message
- Error timestamp
- Consumer or connector name

### 5.3 Poison Messages

A **poison message** is a record that repeatedly crashes or blocks a consumer. Without a strategy, one bad record can stop the whole partition because Kafka preserves order within a partition.

Common strategies:

- Send the record to a DLQ after a fixed number of failures
- Skip the record with a clear audit trail
- Pause the partition and alert an operator
- Fix the consumer and replay from the failed offset

The right answer depends on whether the pipeline values availability or strict processing more.

### 5.4 Replay

Replay is one of Kafka's strongest capabilities. Because records are retained, a consumer can reprocess historical data.

Replay is useful when:

- A downstream index must be rebuilt
- A bug in transformation logic is fixed
- A new consumer needs historical data
- A destination lost data and must be restored

Replay requires care:

- The destination writes should be idempotent
- The consumer should know where to start
- Old event versions may still exist in the topic
- Reprocessing can create heavy load on the destination

Design for replay before you need it. It is much easier to replay a pipeline that already has stable keys and idempotent writes.

---

## 6. Coupling and Agility

Kafka decouples systems, but only if event contracts are designed carefully.

### Tight Coupling

Tight coupling happens when consumers depend heavily on producer internals.

Examples:

- Topic value is a direct database row with unclear meaning
- Consumers depend on every column from the source table
- Field names mirror source-system implementation details
- One producer change forces many consumer changes

Tight coupling is sometimes acceptable for internal, short-lived pipelines, but it becomes painful as more teams and systems depend on the topic.

### Loose Coupling

Loose coupling means producers publish stable business events or stable integration records, and consumers depend on documented contracts rather than source internals.

Examples:

- `CustomerCreated`, `OrderPaid`, `ShipmentDispatched`
- Stable keys and versioned envelopes
- Clear timestamp semantics
- Compatibility rules for new fields

Loose coupling improves agility because producers and consumers can evolve independently.

The trade-off is that loosely coupled events require more design discipline. Someone must own the event contract.

---

## 7. Kafka Connect vs Custom Producer and Consumer

Kafka pipelines can be built with Kafka Connect or with custom application code. Both are valid. The decision depends on the source, destination, transformation needs, and operational model.

### 7.1 Kafka Connect

Kafka Connect is a framework for moving data between Kafka and external systems.

```
Database/File/API -> Connector -> Kafka -> Connector -> Destination
```

Use Kafka Connect when:

- The source or sink is common, such as JDBC, MySQL CDC, S3, Elasticsearch, files, or cloud storage
- The integration pattern is repeatable
- You want standardized offset management
- You want connector configuration instead of custom application code
- Transformations are light or can be handled by connector features
- Operations teams prefer a common runtime for integrations

Kafka Connect provides:

- Distributed workers
- REST API for connector management
- Connector task scaling
- Built-in offset storage
- Error handling features
- Connector plugin ecosystem

Trade-offs:

- Custom business logic is harder than in application code
- Connector behavior depends on plugin quality
- Debugging may require understanding the Connect runtime
- Complex transformations may outgrow simple connector configuration

### 7.2 Custom Producer and Consumer

Custom code gives full control over the pipeline.

Use custom producers or consumers when:

- The source is an unusual API or custom system
- The pipeline has domain-specific business logic
- Transformations are complex
- Destination writes require custom idempotency or transaction handling
- The application must make decisions per record
- Existing connectors do not fit the required behavior

Benefits:

- Maximum flexibility
- Easier to express business logic
- Direct control over retries, batching, logging, and data shape
- Fits naturally into existing application deployments

Trade-offs:

- You own offset handling
- You own retries and failure handling
- You own deployment and scaling
- You own monitoring and operational behavior
- More code means more maintenance

### 7.3 Decision Matrix

| Requirement | Prefer Kafka Connect | Prefer custom code |
|---|---|---|
| Standard database/file/cloud integration | Yes | Sometimes |
| Complex business transformation | Sometimes | Yes |
| Unusual source API | Rarely | Yes |
| Low-code repeatable integration | Yes | No |
| Fine-grained application behavior | No | Yes |
| Standardized operations | Yes | Sometimes |
| Existing connector is mature | Yes | Maybe |
| Destination-specific transaction logic | Sometimes | Yes |

A practical rule:

> If the pipeline is mostly moving data between known systems, start with Kafka Connect. If the pipeline is mostly business logic, write custom code.

---

## 8. Operational Comparison

Design is not only about code. Pipelines must run, fail, recover, scale, and be observed.

| Concern | Kafka Connect | Custom producer/consumer |
|---|---|---|
| Deployment | Deploy connector config to Connect workers | Deploy application/service |
| Offset handling | Managed by Connect | Managed by application |
| Scaling | Increase connector tasks where supported | Add app instances / tune partitions |
| Retries | Connector/framework configuration | Application logic |
| DLQ | Supported by many Connect setups | Application must implement |
| Monitoring | Connect REST API, worker metrics | App logs, metrics, tracing |
| Transformations | Simple SMTs and connector options | Full programming language |
| Testing | Config and integration tests | Unit, integration, and end-to-end tests |

Neither approach removes the need for ownership. Someone must know how to restart it, observe lag, inspect failures, and replay data safely.

---

## 9. Event Design

A Kafka event should be designed as a contract. Consumers should not need private knowledge of the producer's database tables or internal classes to understand the event.

### 9.1 Keys

The key controls partition routing and often identifies the entity.

Good keys:

- Customer id for customer events
- Order id for order lifecycle events
- Device id for sensor readings
- Account id for account balance changes

Key design affects:

- Ordering per entity
- Partition distribution
- Consumer parallelism
- Idempotent writes
- Compacted topic behavior

Avoid keys that create hot partitions. For example, using `country` as a key may send most traffic to one partition if one country dominates the data.

### 9.2 Envelope Structure

An envelope wraps the business payload with metadata.

Example:

```json
{
  "event_id": "7f4a2d8c-7a8d-4e0f-9a41-8d08f2b5a412",
  "event_type": "CustomerUpdated",
  "event_version": 1,
  "source": "crm",
  "occurred_at": "2026-05-10T10:15:30Z",
  "emitted_at": "2026-05-10T10:15:31Z",
  "key": "C001",
  "data": {
    "customer_id": "C001",
    "name": "Alice Tan",
    "country": "Singapore"
  }
}
```

The envelope helps consumers answer:

- What kind of event is this?
- Which system produced it?
- When did the business event happen?
- When was it published?
- Which schema version is this?
- What unique id can be used for deduplication?

### 9.3 Timestamps

Timestamps need clear meaning.

Common timestamp fields:

| Field | Meaning |
|---|---|
| `occurred_at` | When the business event happened |
| `emitted_at` | When the source published the event |
| `processed_at` | When a downstream processor handled it |
| Kafka record timestamp | Timestamp assigned by producer or broker |

Do not rely on a timestamp field without defining what it means. In pipelines, `occurred_at` and `emitted_at` can differ significantly if a source system is delayed or replaying historical data.

### 9.4 Source Metadata

Source metadata helps with debugging and replay.

Useful metadata can include:

- Source system name
- Source table or entity
- Operation type, such as insert, update, delete
- Source transaction id
- Source record id
- Connector or application version
- Trace id or correlation id

Metadata should help operators understand where a record came from without connecting to every source system manually.

### 9.5 Idempotency Keys

An idempotency key identifies a logical event or final destination record so the same input can be processed more than once safely.

Possible idempotency keys:

- `event_id` for event-level deduplication
- `customer_id` for upserting a customer document
- `order_id` for upserting an order state
- `source_table + source_primary_key` for database-derived records

The right key depends on the destination behavior. If the destination is a search index, the document id is often the natural idempotency key. If the destination is an append-only audit table, the event id may be better.

### 9.6 Versioning

Event versioning makes change manageable.

Common approaches:

- Add `event_version` inside the envelope
- Use schema registry compatibility rules
- Create a new event type for a major semantic change
- Keep old fields during a transition period

Avoid changing a field's meaning while keeping the same name and version. That is one of the fastest ways to break consumers silently.

---

## 10. Pipeline Design Checklist

Before implementing a Kafka pipeline, answer these questions:

| Area | Question |
|---|---|
| Timeliness | How fresh does the destination need to be? |
| Source | Does the source emit events, support CDC, or require polling? |
| Reliability | Is duplicate processing acceptable if writes are idempotent? |
| Keys | What key preserves ordering and spreads load evenly? |
| Format | Is JSON enough, or is schema enforcement required? |
| Transformations | Where should data be filtered, enriched, or reshaped? |
| Failures | Which errors are retryable, and where do bad records go? |
| Replay | Can the destination safely handle historical reprocessing? |
| Scaling | How many partitions and consumers are needed? |
| Ownership | Who operates, monitors, and changes the pipeline? |
| Tooling | Is this a Kafka Connect use case or a custom code use case? |

This checklist becomes the bridge to Module 9, where one design is implemented as a real pipeline.

---

## 11. Module 8 Summary

| Concept | Key point |
|---|---|
| Pipeline design | Defines movement of data from source to Kafka to destination |
| Timeliness | Choose batch, micro-batch, or real time based on business need |
| Reliability | Most pipelines use at-least-once delivery plus idempotent writes |
| Throughput | Partitions, batching, compression, and sink capacity shape performance |
| Backpressure | Kafka buffers pressure, but persistent lag must be addressed |
| Data format | JSON is readable; schema-managed formats are stronger for production |
| Transformations | Can happen at source, in a stream processor, or at the sink |
| Failure handling | Retries, DLQs, poison-message handling, and replay must be designed |
| Coupling | Stable event contracts reduce dependency between systems |
| Kafka Connect | Best for standard, repeatable integrations |
| Custom code | Best for business logic, unusual sources, and custom behavior |
| Event envelope | Adds metadata, timestamps, versioning, and idempotency context |

---

## Module 8 Exit Criteria

A learner is ready for Module 9 when:

- [ ] They can explain the difference between batch, micro-batch, and real-time pipelines
- [ ] They can describe why at-least-once delivery often requires idempotent destination writes
- [ ] They can identify common throughput and backpressure controls in Kafka pipelines
- [ ] They can explain when JSON is acceptable and why schema enforcement matters in production
- [ ] They can compare source-side, stream-processor, and sink-side transformations
- [ ] They can describe how retries, DLQs, poison messages, and replay fit into failure handling
- [ ] They can decide whether a use case is better suited for Kafka Connect or custom code
- [ ] They can outline a basic event envelope with key, metadata, timestamps, idempotency key, and version

---

## Further Reading

- Kafka documentation - Kafka Connect
- Kafka documentation - Producer and Consumer configuration
- Kafka documentation - Design and operations
- Confluent Schema Registry documentation
- Martin Kleppmann - Designing Data-Intensive Applications, chapters on logs and stream processing
