# ============================================================
#  Kafka Connect — Custom Image
#  Base: apache/kafka:4.0.0
#  Connectors: MySQL JDBC + Elasticsearch
# ============================================================

FROM apache/kafka:4.0.0

USER root

# Install curl and unzip for downloading/extracting connector zips
RUN apk add --no-cache curl unzip

# ── Create plugin directory ────────────────────────────────
RUN mkdir -p /opt/kafka/plugins

# ── MySQL JDBC Connector v10.9.3 (Confluent Hub zip) ──────
RUN curl -fsSL \
    https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-jdbc/versions/10.9.3/confluentinc-kafka-connect-jdbc-10.9.3.zip \
    -o /tmp/jdbc.zip && \
    unzip -q /tmp/jdbc.zip -d /opt/kafka/plugins/ && \
    rm /tmp/jdbc.zip

# ── MySQL JDBC Driver (added into the plugin's lib dir) ───
RUN curl -fsSL \
    https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.3.0/mysql-connector-j-8.3.0.jar \
    -o /opt/kafka/plugins/confluentinc-kafka-connect-jdbc-10.9.3/lib/mysql-connector-j.jar

# ── Elasticsearch Connector v14.1.0 (Confluent Hub zip) ───
RUN curl -fsSL \
    https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-elasticsearch/versions/14.1.0/confluentinc-kafka-connect-elasticsearch-14.1.0.zip \
    -o /tmp/es.zip && \
    unzip -q /tmp/es.zip -d /opt/kafka/plugins/ && \
    rm /tmp/es.zip

# ── Entrypoint script ─────────────────────────────────────
COPY start-connect.sh /opt/kafka/start-connect.sh
RUN chmod +x /opt/kafka/start-connect.sh

USER appuser

EXPOSE 8083

CMD ["/opt/kafka/start-connect.sh"]