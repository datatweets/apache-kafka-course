#!/usr/bin/env python3
"""
============================================================
  Apache Kafka Course — Environment Verification Script
  Kafka 4.0 | KRaft | 3-Broker Cluster | Kafdrop

  Run this BEFORE the course starts to confirm everything works.
  Usage:  python verify_setup.py
          python verify_setup.py --pipeline   (includes MySQL/ES/Connect)
============================================================
"""

import sys
import time
import uuid
import socket
import argparse
import subprocess
from datetime import datetime, timezone

# ── Try importing optional libs ────────────────────────────
try:
    import urllib.request
    import urllib.error
    import json
except ImportError:
    pass

# ── Colours (no external libs needed) ─────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

def ok(msg):      print(f"  {GREEN}✔{RESET}  {msg}")
def fail(msg):    print(f"  {RED}✖{RESET}  {msg}")
def warn(msg):    print(f"  {YELLOW}⚠{RESET}  {msg}")
def info(msg):    print(f"  {CYAN}ℹ{RESET}  {msg}")
def header(msg):  print(f"\n{BOLD}{CYAN}{msg}{RESET}")
def divider():    print(f"  {'─' * 54}")

# ──────────────────────────────────────────────────────────
#  1. CHECK PYTHON VERSION
# ──────────────────────────────────────────────────────────
def check_python():
    header("[ 1 ] Python Version")
    v = sys.version_info
    if v.major == 3 and v.minor >= 9:
        ok(f"Python {v.major}.{v.minor}.{v.micro} — compatible")
        return True
    else:
        fail(f"Python {v.major}.{v.minor}.{v.micro} — need 3.9+")
        return False

# ──────────────────────────────────────────────────────────
#  2. CHECK PYTHON DEPENDENCIES
# ──────────────────────────────────────────────────────────
REQUIRED_PACKAGES = {
    "kafka":         "kafka-python",
    "confluent_kafka": "confluent-kafka",
    "requests":      "requests",
    "mysql.connector": "mysql-connector-python",
    "elasticsearch": "elasticsearch",
}

def check_dependencies():
    header("[ 2 ] Python Dependencies")
    results = {}
    for module, package in REQUIRED_PACKAGES.items():
        try:
            __import__(module)
            ok(f"{package}")
            results[module] = True
        except ImportError:
            fail(f"{package}  →  pip install {package}")
            results[module] = False
    return results

# ──────────────────────────────────────────────────────────
#  3. CHECK DOCKER
# ──────────────────────────────────────────────────────────
REQUIRED_CONTAINERS = {
    "kafka1":   "9092",
    "kafka2":   "9093",
    "kafka3":   "9094",
    "kafdrop":  "9000",
}

PIPELINE_CONTAINERS = {
    "mysql":         "3307",
    "elasticsearch": "9200",
    "kafka-connect": "8083",
}

def check_docker(include_pipeline=False):
    header("[ 3 ] Docker Containers")

    try:
        result = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}\t{{.Status}}"],
            capture_output=True, text=True, timeout=10
        )
        running = dict(
            line.split("\t") for line in result.stdout.strip().splitlines()
            if "\t" in line
        )
    except (subprocess.TimeoutExpired, FileNotFoundError):
        fail("Docker not found or not running. Install Docker Desktop.")
        return False

    targets = {**REQUIRED_CONTAINERS}
    if include_pipeline:
        targets.update(PIPELINE_CONTAINERS)

    all_ok = True
    for name, port in targets.items():
        if name in running:
            status = running[name]
            if "healthy" in status.lower() or "up" in status.lower():
                ok(f"{name:<20} running  (port {port})")
            else:
                warn(f"{name:<20} {status}")
        else:
            fail(f"{name:<20} NOT running")
            if name in REQUIRED_CONTAINERS:
                all_ok = False

    return all_ok

# ──────────────────────────────────────────────────────────
#  4. CHECK TCP PORTS
# ──────────────────────────────────────────────────────────
def check_port(host, port, timeout=3):
    try:
        with socket.create_connection((host, int(port)), timeout=timeout):
            return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        return False

def check_ports(include_pipeline=False):
    header("[ 4 ] Port Connectivity")

    targets = {
        "Kafka Broker 1": ("localhost", 9092),
        "Kafka Broker 2": ("localhost", 9093),
        "Kafka Broker 3": ("localhost", 9094),
        "Kafdrop UI":     ("localhost", 9000),
    }
    if include_pipeline:
        targets.update({
            "MySQL":         ("localhost", 3307),
            "Elasticsearch": ("localhost", 9200),
            "Kafka Connect": ("localhost", 8083),
        })

    all_ok = True
    for name, (host, port) in targets.items():
        if check_port(host, port):
            ok(f"{name:<22} localhost:{port}  reachable")
        else:
            fail(f"{name:<22} localhost:{port}  UNREACHABLE")
            all_ok = False

    return all_ok

# ──────────────────────────────────────────────────────────
#  5. KAFKA BROKER — PRODUCE & CONSUME ROUND TRIP
# ──────────────────────────────────────────────────────────
def check_kafka_roundtrip():
    header("[ 5 ] Kafka — Produce & Consume Round Trip")

    try:
        from kafka import KafkaAdminClient, KafkaProducer, KafkaConsumer
        from kafka.admin import NewTopic
        from kafka.errors import TopicAlreadyExistsError, KafkaError
    except ImportError:
        warn("kafka-python not installed. Skipping round trip test.")
        warn("Run:  pip install kafka-python")
        return False

    BROKERS    = ["localhost:9092", "localhost:9093", "localhost:9094"]
    TEST_TOPIC = f"_verify-{uuid.uuid4().hex[:8]}"
    TEST_MSG   = f"kafka-course-verify-{datetime.now(timezone.utc).isoformat()}"
    all_ok     = True

    # ── Admin: create topic ──────────────────────────────
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=BROKERS,
            client_id="verify-admin",
            request_timeout_ms=10000,
        )
        admin.create_topics([NewTopic(
            name=TEST_TOPIC,
            num_partitions=3,
            replication_factor=3,
        )])
        ok(f"Topic created: {TEST_TOPIC}  (3 partitions, RF=3)")
    except TopicAlreadyExistsError:
        ok(f"Topic exists:  {TEST_TOPIC}")
    except Exception as e:
        fail(f"Cannot create topic: {e}")
        return False

    # ── Producer ─────────────────────────────────────────
    try:
        producer = KafkaProducer(
            bootstrap_servers=BROKERS,
            value_serializer=lambda v: v.encode("utf-8"),
            acks="all",
            retries=3,
        )
        future = producer.send(TEST_TOPIC, value=TEST_MSG)
        record = future.get(timeout=15)
        producer.flush()
        producer.close()
        ok(f"Message produced → partition {record.partition}, offset {record.offset}")
    except Exception as e:
        fail(f"Producer error: {e}")
        all_ok = False

    # ── Consumer ───���─────────────────────────────────────
    try:
        consumer = KafkaConsumer(
            TEST_TOPIC,
            bootstrap_servers=BROKERS,
            auto_offset_reset="earliest",
            consumer_timeout_ms=10000,
            group_id=f"verify-group-{uuid.uuid4().hex[:6]}",
            value_deserializer=lambda v: v.decode("utf-8"),
        )
        received = None
        for msg in consumer:
            received = msg.value
            break
        consumer.close()

        if received and TEST_MSG in received:
            ok(f"Message consumed  ← '{received[:60]}'")
        else:
            fail("Message NOT received within timeout")
            all_ok = False
    except Exception as e:
        fail(f"Consumer error: {e}")
        all_ok = False

    # ── Cleanup ──────────────────────────────────────────
    try:
        admin.delete_topics([TEST_TOPIC])
        admin.close()
        ok(f"Test topic deleted: {TEST_TOPIC}")
    except Exception:
        warn(f"Could not delete test topic {TEST_TOPIC} (minor, ignore)")

    return all_ok

# ──────────────────────────────────────────────────────────
#  6. KAFDROP UI
# ──────────────────────────────────────────────────────────
def check_kafdrop():
    header("[ 6 ] Kafdrop Web UI")
    url = "http://localhost:9000"
    try:
        req = urllib.request.urlopen(url, timeout=5)
        if req.status == 200:
            ok(f"Kafdrop accessible → {url}")
            return True
    except Exception as e:
        fail(f"Kafdrop unreachable: {e}")
        return False

# ──────────────────────────────────────────────────────────
#  7. PIPELINE CHECKS (--pipeline flag)
# ──────────────────────────────────────────────────────────
def check_mysql():
    header("[ 7a ] MySQL")
    try:
        import mysql.connector
        conn = mysql.connector.connect(
            host="localhost", port=3307,
            user="kafka", password="kafka123",
            database="kafka_course",
            connection_timeout=5,
        )
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM customers;")
        count = cursor.fetchone()[0]
        conn.close()
        ok(f"MySQL connected — kafka_course.customers has {count} rows")
        return True
    except ImportError:
        warn("mysql-connector-python not installed. pip install mysql-connector-python")
        return False
    except Exception as e:
        fail(f"MySQL error: {e}")
        return False

def check_elasticsearch():
    header("[ 7b ] Elasticsearch")
    url = "http://localhost:9200/_cluster/health"
    try:
        req  = urllib.request.urlopen(url, timeout=10)
        data = json.loads(req.read())
        status = data.get("status", "unknown")
        nodes  = data.get("number_of_nodes", 0)
        if status in ("green", "yellow"):
            ok(f"Elasticsearch healthy — status: {status}, nodes: {nodes}")
            return True
        else:
            fail(f"Elasticsearch unhealthy — status: {status}")
            return False
    except Exception as e:
        fail(f"Elasticsearch error: {e}")
        return False

def check_kafka_connect():
    header("[ 7c ] Kafka Connect")
    url = "http://localhost:8083/connectors"
    try:
        req      = urllib.request.urlopen(url, timeout=10)
        data     = json.loads(req.read())
        plugins_req = urllib.request.urlopen("http://localhost:8083/connector-plugins", timeout=10)
        plugins     = json.loads(plugins_req.read())
        names       = [p.get("class", "").split(".")[-1] for p in plugins]
        ok(f"Kafka Connect running — {len(data)} connectors active")
        ok(f"Plugins loaded: {len(plugins)} ({', '.join(names[:4])}{'...' if len(names)>4 else ''})")
        return True
    except Exception as e:
        fail(f"Kafka Connect error: {e}")
        return False

# ──────────────────────────────────────────────────────────
#  8. CLUSTER METADATA
# ──────────────────────────────────────────────────────────
def check_cluster_metadata():
    header("[ 8 ] Cluster Metadata")
    try:
        from kafka import KafkaAdminClient
        admin = KafkaAdminClient(
            bootstrap_servers=["localhost:9092","localhost:9093","localhost:9094"],
            client_id="verify-meta",
            request_timeout_ms=10000,
        )
        topics = admin.list_topics()
        admin.close()

        user_topics  = [t for t in topics if not t.startswith("_")]
        sys_topics   = [t for t in topics if t.startswith("_")]

        ok(f"Broker reachable via all 3 bootstrap servers")
        ok(f"User topics   : {len(user_topics)}")
        ok(f"System topics : {len(sys_topics)}")

        if user_topics:
            info(f"Topics: {', '.join(sorted(user_topics)[:5])}{'...' if len(user_topics)>5 else ''}")
        return True
    except Exception as e:
        fail(f"Metadata fetch error: {e}")
        return False

# ──────────────────────────────────────────────────────────
#  MAIN
# ──────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Apache Kafka Course — Environment Verification"
    )
    parser.add_argument(
        "--pipeline", action="store_true",
        help="Also verify MySQL, Elasticsearch, and Kafka Connect"
    )
    args = parser.parse_args()

    print(f"\n{BOLD}{'=' * 58}")
    print(f"  Apache Kafka 4.0 — Course Environment Verification")
    if args.pipeline:
        print(f"  Mode: FULL (including pipeline services)")
    else:
        print(f"  Mode: CORE (brokers + Kafdrop)")
    print(f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'=' * 58}{RESET}")

    results = {}

    results["python"]       = check_python()
    deps                    = check_dependencies()
    results["dependencies"] = all(deps.values())
    results["docker"]       = check_docker(args.pipeline)
    results["ports"]        = check_ports(args.pipeline)
    results["kafka"]        = check_kafka_roundtrip()
    results["kafdrop"]      = check_kafdrop()
    results["metadata"]     = check_cluster_metadata()

    if args.pipeline:
        results["mysql"]         = check_mysql()
        results["elasticsearch"] = check_elasticsearch()
        results["connect"]       = check_kafka_connect()

    # ── Summary ───────────────────────────────────────────
    print(f"\n{BOLD}{'=' * 58}")
    print(f"  SUMMARY")
    print(f"{'=' * 58}{RESET}")

    passed = sum(1 for v in results.values() if v)
    total  = len(results)

    for check, result in results.items():
        status = f"{GREEN}PASS{RESET}" if result else f"{RED}FAIL{RESET}"
        print(f"  {status}  {check}")

    divider()

    if passed == total:
        print(f"\n  {GREEN}{BOLD}✔ All {total} checks passed — environment ready!{RESET}")
        print(f"\n  {CYAN}Open Kafdrop : http://localhost:9000{RESET}")
        if args.pipeline:
            print(f"  {CYAN}Kafka Connect: http://localhost:8083/connectors{RESET}")
            print(f"  {CYAN}Elasticsearch: http://localhost:9200{RESET}")
    else:
        failed = total - passed
        print(f"\n  {RED}{BOLD}✖ {failed} check(s) failed out of {total}{RESET}")
        print(f"\n  {YELLOW}Fix the issues above then re-run this script.{RESET}")

    print()
    return 0 if passed == total else 1

if __name__ == "__main__":
    sys.exit(main())