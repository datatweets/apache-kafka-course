# Apache Kafka Course Platform

<p align="center">
  <img src="https://img.shields.io/badge/Apache%20Kafka-4.0-231F20?logo=apachekafka&logoColor=white" alt="Apache Kafka 4.0" />
  <img src="https://img.shields.io/badge/KRaft-No%20ZooKeeper-0A66C2" alt="Kafka KRaft mode" />
  <img src="https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white" alt="Docker Compose" />
  <img src="https://img.shields.io/badge/Python-3.9%2B-3776AB?logo=python&logoColor=white" alt="Python 3.9+" />
  <img src="https://img.shields.io/badge/MySQL-8.0-4479A1?logo=mysql&logoColor=white" alt="MySQL 8.0" />
  <img src="https://img.shields.io/badge/Elasticsearch-8.11-005571?logo=elasticsearch&logoColor=white" alt="Elasticsearch 8.11" />
</p>

This repository contains the local platform, course notes, and Python labs for a two-day Apache Kafka course.

The platform runs a real **3-broker Kafka 4.0 KRaft cluster** on Docker Desktop, plus optional MySQL, Elasticsearch, and Kafka Connect services for pipeline labs.

> Detailed installation and first-run walkthrough lives in [Module 1-3: Kafka Foundation and KRaft](docs/module-01-03-kafka-foundation-and-kraft.md).  
> This README is the fast project entry point, not a duplicate of the full setup lesson.

---

## What You Get

| Area | Included |
|---|---|
| Kafka cluster | 3 Kafka 4.0 brokers in KRaft mode: `kafka1`, `kafka2`, `kafka3` |
| UI | Kafdrop at `http://localhost:9000` |
| Python labs | Producers, consumers, reliability, pipeline, monitoring, and stream-processing scripts |
| Pipeline services | Optional MySQL, Elasticsearch, Kafka Connect profile |
| Course docs | Modules 1-9 and 11-13 currently written; Module 10 planned |
| Verification | Python setup checker and Kafka round-trip test |

---

## Repository Layout

```text
docker/
  docker-compose.yml              # Kafka, Kafdrop, optional pipeline services
  .env                            # Versions, ports, heap sizes, cluster id
  connect/                        # Kafka Connect image
  init-sql/                       # MySQL seed data
  scripts/                        # Topic creation and verification tools

docs/
  module-01-03-kafka-foundation-and-kraft.md
  module-04-kafka-producers.md
  module-05-kafka-consumers.md
  module-06-kafka-internals.md
  module-07-reliable-data-delivery.md
  module-08-designing-kafka-data-pipelines.md
  module-09-kafka-pipeline.md
  module-11-administering-kafka.md
  module-12-kafka-apis-monitoring.md
  module-13-stream-processing.md

python_scripts/
  module04/                       # Producer labs
  module05/                       # Consumer labs
  module07/                       # Reliable delivery labs
  module09/                       # MySQL -> Kafka -> Elasticsearch pipeline
  module12/                       # Kafka API and monitoring scripts
  module13/                       # Faust stream processing labs

requirements.txt
README.md
```

---

## Quick Start

Prerequisites:

- Docker Desktop
- Python 3.9 or newer
- Git
- 6 GB Docker memory recommended; 4 GB can work with smaller heap values in `docker/.env`

Run from the repository root:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Start Kafka:

```bash
cd docker
docker compose up -d
bash scripts/create-topics.sh
cd ..
python docker/scripts/verify_setup.py
```

Open:

```text
http://localhost:9000
```

Windows users should follow the exact PowerShell commands in [Module 1-3](docs/module-01-03-kafka-foundation-and-kraft.md), especially for virtualenv activation and topic creation.

---

## Platform Services

| Service | Purpose | Host access | Internal Docker access |
|---|---|---|---|
| `kafka1` | Broker + controller | `localhost:9092` | `kafka1:29092` |
| `kafka2` | Broker + controller | `localhost:9093` | `kafka2:29093` |
| `kafka3` | Broker + controller | `localhost:9094` | `kafka3:29094` |
| `kafdrop` | Kafka UI | `http://localhost:9000` | `kafdrop:9000` |
| `mysql` | Pipeline source DB | `localhost:3307` | `mysql:3306` |
| `elasticsearch` | Pipeline sink | `http://localhost:9200` | `elasticsearch:9200` |
| `kafka-connect` | Connector runtime | `http://localhost:8083` | `kafka-connect:8083` |

Important address rule:

- Use `localhost:9092,localhost:9093,localhost:9094` from Python scripts running on your host machine.
- Use `kafka1:29092` from commands running inside containers with `docker exec`.

---

## Optional Pipeline Stack

Modules 8 and 9 use the optional pipeline services.

Start them from `docker/`:

```bash
docker compose --profile pipeline up -d --build
```

Verify everything:

```bash
cd ..
source .venv/bin/activate
python docker/scripts/verify_setup.py --pipeline
```

The first Kafka Connect build downloads connector plugins and needs internet access.

---

## Course Module Map

| Module | Status | Document | Code |
|---|---|---|---|
| 1-3: Introduction, installation, KRaft | Ready | [docs/module-01-03-kafka-foundation-and-kraft.md](docs/module-01-03-kafka-foundation-and-kraft.md) | `docker/`, `docker/scripts/` |
| 4: Producers | Ready | [docs/module-04-kafka-producers.md](docs/module-04-kafka-producers.md) | [python_scripts/module04](python_scripts/module04) |
| 5: Consumers | Ready | [docs/module-05-kafka-consumers.md](docs/module-05-kafka-consumers.md) | [python_scripts/module05](python_scripts/module05) |
| 6: Kafka Internals | Ready | [docs/module-06-kafka-internals.md](docs/module-06-kafka-internals.md) | CLI lab with Docker/Kafka tools |
| 7: Reliable Data Delivery | Ready | [docs/module-07-reliable-data-delivery.md](docs/module-07-reliable-data-delivery.md) | [python_scripts/module07](python_scripts/module07) |
| 8: Designing Kafka Data Pipelines | Ready | [docs/module-08-designing-kafka-data-pipelines.md](docs/module-08-designing-kafka-data-pipelines.md) | Design-focused |
| 9: Real-Time Pipeline | Ready | [docs/module-09-kafka-pipeline.md](docs/module-09-kafka-pipeline.md) | [python_scripts/module09](python_scripts/module09) |
| 10: Cross-Cluster Data Mirroring | Planned | Coming later | Coming later |
| 11: Administering Kafka | Ready | [docs/module-11-administering-kafka.md](docs/module-11-administering-kafka.md) | CLI-focused |
| 12: Kafka APIs and Monitoring | Ready | [docs/module-12-kafka-apis-monitoring.md](docs/module-12-kafka-apis-monitoring.md) | [python_scripts/module12](python_scripts/module12), CLI/API |
| 13: Stream Processing | Ready | [docs/module-13-stream-processing.md](docs/module-13-stream-processing.md) | [python_scripts/module13](python_scripts/module13) |

---

## Running Module Scripts

Activate the virtual environment from the repository root:

```bash
source .venv/bin/activate
```

Examples:

```bash
python python_scripts/module04/01_simple_producer.py
python python_scripts/module05/01_simple_consumer.py
python python_scripts/module07/01_reliable_producer.py
python python_scripts/module07/02_reliable_consumer.py --max-messages 8
python python_scripts/module12/01_kafka_api_health_check.py
python python_scripts/module09/01_explore_mysql_source.py
python python_scripts/module13/01_stream_producer.py
```

For Module 9, start the pipeline profile first.

For Module 13, open four terminals from the repository root:

```bash
python python_scripts/module13/01_stream_producer.py
python python_scripts/module13/02_stateless_transform.py worker -l info
python python_scripts/module13/03_word_count.py worker -l info --web-port 6067
python python_scripts/module13/04_stream_monitor.py
```

---

## Useful Kafka Commands

List topics:

```bash
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:29092 \
  --list
```

Describe a topic:

```bash
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:29092 \
  --describe \
  --topic m7-reliable-topic
```

Produce from the console:

```bash
docker exec -it kafka1 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka1:29092 \
  --topic m4-simple-topic
```

Consume from the console:

```bash
docker exec -it kafka1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka1:29092 \
  --topic m4-simple-topic \
  --from-beginning
```

Inspect KRaft controller quorum:

```bash
docker exec kafka1 /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server kafka1:29092 \
  describe --status
```

---

## Stop, Restart, Reset

Stop containers but keep data:

```bash
cd docker
docker compose --profile pipeline down
```

Start again:

```bash
docker compose --profile pipeline up -d
```

Delete containers and all data volumes:

```bash
docker compose --profile pipeline down -v
```

Use `down -v` only when you intentionally want a clean reset.

Important: do not change `CLUSTER_ID` in `docker/.env` after volumes have been created. If you need a new cluster id, run `docker compose down -v` first.

---

## Troubleshooting

Docker daemon not available:

```text
Cannot connect to the Docker daemon
```

Start Docker Desktop and wait until it is ready.

Kafka containers not healthy:

```bash
cd docker
docker compose ps
docker logs kafka1 --tail 100
```

Python dependency error:

```bash
source .venv/bin/activate
python -m pip install -r requirements.txt
```

Kafka Connect plugin build failure:

```bash
cd docker
docker compose --profile pipeline build kafka-connect
docker compose --profile pipeline up -d
```

Port conflict:

- Change ports in `docker/.env`, or stop the other process using the port.

---

## Notes For Instructors

- Keep the README short and operational.
- Teach detailed installation from [Module 1-3](docs/module-01-03-kafka-foundation-and-kraft.md).
- Use Module 6 before Module 7; reliability settings make more sense after leaders, replicas, and ISR are visible.
- Use Module 8 as the pipeline design bridge before the Module 9 implementation.
- Module 10 remains planned; Modules 11-13 are available for the current course path.
