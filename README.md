# Apache Kafka Course Platform

This repository contains the local platform used for the Apache Kafka course. It runs on Docker Desktop and gives every student the same environment on macOS or Windows:

- 3 Kafka 4.0 brokers running in KRaft mode (no ZooKeeper)
- Kafdrop web UI
- Optional MySQL, Elasticsearch, and Kafka Connect services for pipeline labs
- Python verification and tutorial scripts

The main Docker files are under `docker/`. Run Docker Compose commands from that directory unless a command says otherwise.

## 1. Requirements

Install these before the course starts.

### macOS

1. Install Docker Desktop for Mac.
2. Install Python 3.9 or newer.
3. Install Git.
4. Make sure Docker Desktop is running.
5. In Docker Desktop, allocate at least 6 GB memory if possible. For lower-memory laptops, use 4 GB and reduce heap sizes in `docker/.env`.

### Windows

1. Install Docker Desktop for Windows.
2. During Docker Desktop setup, enable WSL 2 if Docker asks for it.
3. Install Python 3.9 or newer from python.org or the Microsoft Store.
4. Install Git for Windows.
5. Use PowerShell for the commands in this README.
6. Make sure Docker Desktop is running.
7. In Docker Desktop, allocate at least 6 GB memory if possible. For lower-memory laptops, use 4 GB and reduce heap sizes in `docker/.env`.

## 2. Clone the Repository

macOS:

```bash
git clone https://github.com/datatweets/apache-kafka-course.git
cd apache-kafka-course
```

Windows PowerShell:

```powershell
git clone https://github.com/datatweets/apache-kafka-course.git
cd apache-kafka-course
```

The expected structure after cloning:

```text
docker/
  docker-compose.yml
  .env
  connect/
  init-sql/
  scripts/
docs/
python_pipeline/
requirements.txt
README.md
```

## 3. Create the Python Environment

Create a fresh virtual environment on your machine using `requirements.txt`. Each learner must do this step on their own OS — the `.venv` folder is not committed to the repository.

macOS:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Windows PowerShell:

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

If PowerShell blocks activation, run:

```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
.\.venv\Scripts\Activate.ps1
```

## 4. Configure the Platform

Open `docker/.env` if you need to tune memory or ports.

> **Critical — do not change `CLUSTER_ID` after the first startup.**
> The `CLUSTER_ID` in `docker/.env` is a fixed base64-encoded UUID that identifies your Kafka cluster.
> Kafka 4 (KRaft mode) writes this ID into the broker data volumes on first boot.
> If you change `CLUSTER_ID` after volumes are created, all three brokers will refuse to start because the ID in their volume metadata will no longer match the environment variable.
> If you need a completely fresh cluster, run `docker compose down -v` first to delete the volumes, then you may change the ID.
> To generate a new valid ID:
> ```bash
> python -c "import uuid,base64; print(base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip('='))"
> ```

Default host ports:

| Service | URL or port |
| --- | --- |
| Kafka broker 1 | `localhost:9092` |
| Kafka broker 2 | `localhost:9093` |
| Kafka broker 3 | `localhost:9094` |
| Kafdrop | `http://localhost:9000` |
| MySQL | `localhost:3307` |
| Elasticsearch | `http://localhost:9200` |
| Kafka Connect | `http://localhost:8083` |

For laptops with limited memory, edit `docker/.env`:

```env
KAFKA_HEAP=384m
KAFKA_HEAP_MIN=256m
CONNECT_HEAP=384m
ES_HEAP=256m
```

Do not change `CLUSTER_ID` after the first successful startup unless you also delete the Docker volumes.

## 5. Start the Core Kafka Platform

Run from the `docker/` directory:

macOS:

```bash
cd docker
docker compose up -d
```

Windows PowerShell:

```powershell
cd docker
docker compose up -d
```

Wait until the containers become healthy:

```bash
docker compose ps
```

Expected core containers:

- `kafka1`
- `kafka2`
- `kafka3`
- `kafdrop`

Open Kafdrop:

```text
http://localhost:9000
```

## 6. Create Course Topics

Run from the `docker/` directory:

macOS:

```bash
bash scripts/create-topics.sh
```

Windows PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/create-topics.ps1
```

The Bash script also works on Windows if you run it from Git Bash:

```bash
bash scripts/create-topics.sh
```

After topics are created, refresh Kafdrop and confirm that the course topics are visible.

## 7. Verify the Core Setup

Go back to the repository root and activate the Python environment.

macOS:

```bash
cd ..
source .venv/bin/activate
python docker/scripts/verify_setup.py
```

Windows PowerShell:

```powershell
cd ..
.\.venv\Scripts\Activate.ps1
python docker\scripts\verify_setup.py
```

The script checks Python, dependencies, Docker containers, ports, Kafka produce/consume, Kafdrop, and cluster metadata.

## 8. Start Optional Pipeline Services

Use this only for labs that need MySQL, Elasticsearch, or Kafka Connect.

Run from the `docker/` directory:

```bash
docker compose --profile pipeline up -d --build
```

The first build downloads Kafka Connect plugins, so it needs internet access and can take several minutes.

Verify the full setup from the repository root:

macOS:

```bash
cd ..
source .venv/bin/activate
python docker/scripts/verify_setup.py --pipeline
```

Windows PowerShell:

```powershell
cd ..
.\.venv\Scripts\Activate.ps1
python docker\scripts\verify_setup.py --pipeline
```

Useful checks:

```bash
curl http://localhost:9200
curl http://localhost:8083/connectors
```

PowerShell alternative:

```powershell
Invoke-RestMethod http://localhost:9200
Invoke-RestMethod http://localhost:8083/connectors
```

## 9. Basic Kafka Commands

List topics:

```bash
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Describe a topic:

```bash
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic m4-simple-topic
```

Produce messages:

```bash
docker exec -it kafka1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic m4-simple-topic
```

Consume messages:

```bash
docker exec -it kafka1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic m4-simple-topic --from-beginning
```

## 10. Stop or Reset

Stop containers but keep data:

```bash
cd docker
docker compose --profile pipeline down
```

Start again with existing data:

```bash
docker compose --profile pipeline up -d
```

Delete containers and all course data:

```bash
docker compose --profile pipeline down -v
```

Use `down -v` only when you intentionally want a clean reset. It deletes Kafka, MySQL, and Elasticsearch volumes.

## 11. Troubleshooting

Docker is not running:

```text
Cannot connect to the Docker daemon
```

Start Docker Desktop, wait until it is ready, then run the command again.

Port already in use:

```text
Bind for 0.0.0.0:9000 failed
```

Stop the other application using the port or change the matching port in `docker/.env`.

Containers are unhealthy:

```bash
docker compose ps
docker logs kafka1 --tail 100
docker logs kafka-connect --tail 100
```

Python package import fails:

```bash
python -m pip install -r requirements.txt
```

Kafka Connect build fails while downloading plugins:

1. Confirm internet access.
2. Re-run:

```bash
cd docker
docker compose --profile pipeline build kafka-connect
docker compose --profile pipeline up -d
```

Windows path or script issues:

- Prefer PowerShell for Docker and Python commands.
- Use Git Bash only for Bash scripts such as `scripts/create-topics.sh`.
- Do not copy the macOS `.venv` to Windows. Recreate it with `requirements.txt`.

## 12. Python Pipeline Tutorial

The Python tutorial for `MySQL -> Kafka -> Elasticsearch` is in:

```text
docs/python-mysql-kafka-elasticsearch.md
```

It uses the services from this repository and the packages in `requirements.txt`.
