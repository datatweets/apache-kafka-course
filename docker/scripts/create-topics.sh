#!/bin/bash
# ============================================================
#  Create all course topics on the 3-broker KRaft cluster
#  Run after: docker compose up -d
#  Usage: bash scripts/create-topics.sh
# ============================================================

BOOTSTRAP="localhost:9092"
REPLICATION=3
PARTITIONS=3

TOPICS=(
  # Module 4 — Producers
  "m4-simple-topic"
  "m4-partitioned-topic"
  "m4-keyed-topic"
  # Module 5 — Consumers
  "m5-consumer-topic"
  "m5-group-topic"
  "m5-offsets-topic"
  # Module 6 — Internals
  "m6-replication-topic"
  "m6-isr-topic"
  # Module 7 — Reliability
  "m7-reliable-topic"
  "m7-exactly-once-topic"
  # Module 8 — Pipelines
  "m8-pipeline-topic"
  # Module 9 — Connect
  "m9-mysql-customers"
  "m9-mysql-orders"
  "m9-elasticsearch-sink"
  # Module 11 — Admin
  "m11-admin-topic"
  "m11-dynamic-config-topic"
  # Module 12 — Monitoring
  "m12-metrics-topic"
  # Module 13 — Streams
  "m13-stream-input"
  "m13-stream-output"
  "m13-wordcount-input"
  "m13-wordcount-output"
)

echo ""
echo "=============================================="
echo "  Creating Kafka Course Topics"
echo "  Bootstrap: ${BOOTSTRAP}"
echo "  Replication: ${REPLICATION}  Partitions: ${PARTITIONS}"
echo "=============================================="
echo ""

SUCCESS=0
FAILED=0

for TOPIC in "${TOPICS[@]}"; do
  echo -n "  Creating: ${TOPIC} ... "
  docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "${BOOTSTRAP}" \
    --create \
    --if-not-exists \
    --topic "${TOPIC}" \
    --partitions "${PARTITIONS}" \
    --replication-factor "${REPLICATION}" > /dev/null 2>&1

  if [ $? -eq 0 ]; then
    echo "OK"
    ((SUCCESS++))
  else
    echo "FAILED"
    ((FAILED++))
  fi
done

echo ""
echo "=============================================="
echo "  Done: ${SUCCESS} created, ${FAILED} failed"
echo "=============================================="
echo ""
echo "  View topics in Kafdrop: http://localhost:9000"
echo ""