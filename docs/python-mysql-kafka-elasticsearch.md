# Python Tutorial: MySQL -> Kafka -> Elasticsearch

This tutorial builds a small Python pipeline using the platform in this repository.

Data flow:

```text
MySQL tables -> Python producer -> Kafka topics -> Python consumer -> Elasticsearch indexes
```

This tutorial uses Python packages already represented by the repository virtual environment:

- `mysql-connector-python`
- `kafka-python`
- `requests`
- `elasticsearch`

The runnable scripts are in `python_pipeline/`.

## 1. Start the Platform

From the repository root:

macOS:

```bash
source .venv/bin/activate
cd docker
docker compose --profile pipeline up -d --build
cd ..
```

Windows PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
cd docker
docker compose --profile pipeline up -d --build
cd ..
```

Verify everything:

```bash
python docker/scripts/verify_setup.py --pipeline
```

You should have:

- Kafka at `localhost:9092`, `localhost:9093`, `localhost:9094`
- MySQL at `localhost:3307`
- Elasticsearch at `http://localhost:9200`
- Kafdrop at `http://localhost:9000`

## 2. Understand the Source Tables

MySQL starts with two tables from `docker/init-sql/01-init.sql`:

- `customers`
- `orders`

Connect to MySQL from inside the container:

```bash
docker exec -it mysql mysql -ukafka -pkafka123 kafka_course
```

Run:

```sql
SELECT * FROM customers;
SELECT * FROM orders;
```

Exit MySQL:

```sql
exit
```

## 3. Create Kafka Topics

The Python pipeline uses separate topics from the course module topics:

```bash
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic python.mysql.customers --partitions 3 --replication-factor 3
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic python.mysql.orders --partitions 3 --replication-factor 3
```

Confirm:

```bash
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

## 4. Script 1: MySQL to Kafka

The producer script is:

```text
python_pipeline/mysql_to_kafka.py
```

It:

1. Connects to MySQL.
2. Reads rows from `customers` and `orders`.
3. Wraps each row in a JSON event.
4. Publishes customer events to `python.mysql.customers`.
5. Publishes order events to `python.mysql.orders`.

Run one snapshot pass:

```bash
python python_pipeline/mysql_to_kafka.py --once
```

Expected output:

```text
published customers id=1 to python.mysql.customers
published orders id=1 to python.mysql.orders
```

Inspect the Kafka topic:

```bash
docker exec -it kafka1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic python.mysql.customers --from-beginning --max-messages 5
```

## 5. Script 2: Kafka to Elasticsearch

The consumer script is:

```text
python_pipeline/kafka_to_elasticsearch.py
```

It:

1. Subscribes to `python.mysql.customers` and `python.mysql.orders`.
2. Reads JSON events from Kafka.
3. Creates Elasticsearch indexes if needed.
4. Writes documents to:
   - `python-customers`
   - `python-orders`

Run it for the seeded demo rows:

```bash
python python_pipeline/kafka_to_elasticsearch.py --max-messages 10
```

Expected output:

```text
indexed topic=python.mysql.customers id=1 into python-customers
indexed topic=python.mysql.orders id=1 into python-orders
```

Search Elasticsearch:

```bash
curl "http://localhost:9200/python-customers/_search?pretty"
curl "http://localhost:9200/python-orders/_search?pretty"
```

PowerShell:

```powershell
Invoke-RestMethod "http://localhost:9200/python-customers/_search?pretty"
Invoke-RestMethod "http://localhost:9200/python-orders/_search?pretty"
```

## 6. Run the Pipeline Continuously

Open two terminals from the repository root.

Terminal 1, MySQL producer:

```bash
source .venv/bin/activate
python python_pipeline/mysql_to_kafka.py
```

Windows PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
python python_pipeline\mysql_to_kafka.py
```

Terminal 2, Elasticsearch sink:

```bash
source .venv/bin/activate
python python_pipeline/kafka_to_elasticsearch.py
```

Windows PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
python python_pipeline\kafka_to_elasticsearch.py
```

The producer polls MySQL every 5 seconds. The consumer keeps reading Kafka and writing to Elasticsearch.

## 7. Insert New Demo Data

Open a third terminal from the repository root.

macOS:

```bash
source .venv/bin/activate
python python_pipeline/insert_demo_data.py
```

Windows PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
python python_pipeline\insert_demo_data.py
```

You should see:

```text
inserted customer id=6, order id=6
```

Within a few seconds, Terminal 1 should publish the new rows and Terminal 2 should index them.

Search again:

```bash
curl "http://localhost:9200/python-customers/_search?pretty&q=data.name:Python"
curl "http://localhost:9200/python-orders/_search?pretty&q=data.product:Kafka"
```

## 8. How the Event Looks

Each Kafka message is JSON:

```json
{
  "source": "mysql",
  "table": "customers",
  "operation": "snapshot_or_insert",
  "emitted_at": "2026-05-10T00:00:00+00:00",
  "data": {
    "id": 1,
    "name": "Alice Tan",
    "email": "alice@example.com",
    "country": "Singapore",
    "created_at": "2026-05-10T00:00:00",
    "updated_at": "2026-05-10T00:00:00"
  }
}
```

The Kafka message key is the MySQL row `id`. The Elasticsearch document id is also the row `id`, so reprocessing the same event updates the same document instead of creating duplicates.

## 9. Important Teaching Notes

This tutorial uses polling by increasing `id`. That keeps the code easy to understand, but it is not full change data capture.

Limitations:

- Updates to existing rows are not captured.
- Deletes are not captured.
- The script keeps offsets only in memory.
- Restarting the producer sends the seeded rows again.

Production systems usually use CDC tooling such as Debezium or Kafka Connect, durable offset storage, schemas, retries, dead-letter queues, and monitoring.

## 10. Clean Up Tutorial Data

Delete only the tutorial Elasticsearch indexes:

```bash
curl -X DELETE http://localhost:9200/python-customers
curl -X DELETE http://localhost:9200/python-orders
```

PowerShell:

```powershell
Invoke-RestMethod -Method Delete http://localhost:9200/python-customers
Invoke-RestMethod -Method Delete http://localhost:9200/python-orders
```

Delete only the tutorial Kafka topics:

```bash
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic python.mysql.customers
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic python.mysql.orders
```

Stop the full platform:

```bash
cd docker
docker compose --profile pipeline down
```
