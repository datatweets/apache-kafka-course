#!/bin/bash
# ============================================================
#  Kafka Connect startup script
#  Generates connect-distributed.properties from env vars
# ============================================================
set -e

CONFIG_FILE="/tmp/connect-distributed.properties"

echo ">> Generating Kafka Connect configuration..."

cat > "${CONFIG_FILE}" <<EOF
# Bootstrap
bootstrap.servers=${CONNECT_BOOTSTRAP_SERVERS}

# Worker identity
group.id=${CONNECT_GROUP_ID}

# REST API
rest.advertised.host.name=${CONNECT_REST_ADVERTISED_HOST_NAME}
rest.port=${CONNECT_REST_PORT}

# Converters
key.converter=${CONNECT_KEY_CONVERTER}
value.converter=${CONNECT_VALUE_CONVERTER}
key.converter.schemas.enable=${CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE}
value.converter.schemas.enable=${CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE}

# Internal topics
offset.storage.topic=${CONNECT_OFFSET_STORAGE_TOPIC}
offset.storage.replication.factor=${CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR}
offset.flush.interval.ms=${CONNECT_OFFSET_FLUSH_INTERVAL_MS}

config.storage.topic=${CONNECT_CONFIG_STORAGE_TOPIC}
config.storage.replication.factor=${CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR}

status.storage.topic=${CONNECT_STATUS_STORAGE_TOPIC}
status.storage.replication.factor=${CONNECT_STATUS_STORAGE_REPLICATION_FACTOR}

# Plugins
plugin.path=${CONNECT_PLUGIN_PATH}
EOF

echo ">> Waiting for Kafka brokers to be ready..."
until /opt/kafka/bin/kafka-broker-api-versions.sh \
    --bootstrap-server "${CONNECT_BOOTSTRAP_SERVERS}" > /dev/null 2>&1; do
  echo "   ... brokers not ready, retrying in 5s"
  sleep 5
done

echo ">> Kafka brokers ready. Starting Kafka Connect..."
exec /opt/kafka/bin/connect-distributed.sh "${CONFIG_FILE}"