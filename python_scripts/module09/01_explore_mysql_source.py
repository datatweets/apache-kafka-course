#!/usr/bin/env python3
r"""
Module 9 — Script 01: Explore the MySQL Source
===============================================
Before building a pipeline, always understand your source data.
This script connects to MySQL, inspects the schema, and prints
sample rows from the two tables the pipeline will ingest.

Concepts covered:
  - Connecting to MySQL from a Python host script (not from inside Docker)
  - Inspecting table schema with DESCRIBE
  - Reading seed rows to understand what will flow through Kafka

Prerequisites:
  - Docker platform running with the pipeline profile:
      cd docker && docker compose --profile pipeline up -d --build
  - Virtual environment activated:
      source .venv/bin/activate          # macOS
      .\.venv\Scripts\Activate.ps1       # Windows PowerShell

Run:
  python 01_explore_mysql_source.py

The MySQL container maps its internal port 3306 to localhost:3307.
We connect to localhost:3307 because this script runs on the host machine.
"""

import mysql.connector

# ---------------------------------------------------------------------------
# Connection configuration.
# These credentials match the values in docker/docker-compose.yml:
#   MYSQL_USER: kafka
#   MYSQL_PASSWORD: kafka123
#   MYSQL_DATABASE: kafka_course
#
# Port 3307 on the host maps to port 3306 inside the MySQL container.
# ---------------------------------------------------------------------------
MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3307,
    "user": "kafka",
    "password": "kafka123",
    "database": "kafka_course",
}


def connect():
    """Return an open MySQL connection."""
    return mysql.connector.connect(**MYSQL_CONFIG)


def describe_table(cursor, table_name: str):
    """Print the column definitions of a table."""
    cursor.execute(f"DESCRIBE {table_name}")
    rows = cursor.fetchall()
    print(f"\n  Table: {table_name}")
    print(f"  {'Field':<20} {'Type':<25} {'Null':<6} {'Key':<6} {'Default':<15} Extra")
    print(f"  {'-'*20} {'-'*25} {'-'*6} {'-'*6} {'-'*15} -----")
    for row in rows:
        field, typ, null, key, default, extra = row
        print(f"  {field:<20} {str(typ):<25} {null:<6} {key:<6} {str(default):<15} {extra}")


def show_rows(cursor, table_name: str, limit: int = 5):
    """Print the first N rows from a table."""
    cursor.execute(f"SELECT * FROM {table_name} LIMIT %s", (limit,))
    rows = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]

    print(f"\n  Sample rows from '{table_name}' (up to {limit}):")
    print(f"  {' | '.join(f'{col:<15}' for col in column_names)}")
    print(f"  {'-' * (len(column_names) * 17)}")
    for row in rows:
        print(f"  {' | '.join(f'{str(v):<15}' for v in row)}")


def count_rows(cursor, table_name: str) -> int:
    """Return the total row count for a table."""
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    return cursor.fetchone()[0]


def main():
    print("=" * 60)
    print("  Module 9 — MySQL Source Explorer")
    print("=" * 60)

    # -------------------------------------------------------------------
    # Open one connection and reuse the cursor for all queries.
    # In production, use a connection pool (e.g. mysql.connector.pooling)
    # rather than a single persistent connection.
    # -------------------------------------------------------------------
    connection = connect()
    cursor = connection.cursor()

    try:
        # ---------------------------------------------------------------
        # List all tables in the database so learners can see what exists.
        # ---------------------------------------------------------------
        cursor.execute("SHOW TABLES")
        tables = [row[0] for row in cursor.fetchall()]
        print(f"\nDatabase '{MYSQL_CONFIG['database']}' contains {len(tables)} table(s): {tables}")

        for table in ("customers", "orders"):
            # Schema inspection — understand the shape of each row.
            describe_table(cursor, table)

            # Row count — gives a sense of data volume.
            total = count_rows(cursor, table)
            print(f"\n  Total rows in '{table}': {total}")

            # Sample rows — the actual data the pipeline will publish.
            show_rows(cursor, table, limit=5)

        # ---------------------------------------------------------------
        # Show the JOIN between orders and customers so learners understand
        # the data relationships before they decide how to key Kafka records.
        # ---------------------------------------------------------------
        print("\n\n  Orders joined with customer names (first 5):")
        cursor.execute("""
            SELECT o.id, c.name, o.product, o.amount, o.status, o.created_at
            FROM orders o
            JOIN customers c ON o.customer_id = c.id
            ORDER BY o.id
            LIMIT 5
        """)
        joined_rows = cursor.fetchall()
        headers = ["order_id", "customer", "product", "amount", "status", "created_at"]
        print(f"  {' | '.join(f'{h:<15}' for h in headers)}")
        print(f"  {'-' * (len(headers) * 17)}")
        for row in joined_rows:
            print(f"  {' | '.join(f'{str(v):<15}' for v in row)}")

    finally:
        cursor.close()
        connection.close()

    print("\n" + "=" * 60)
    print("  Done.  Next: run 02_mysql_to_kafka_producer.py")
    print("=" * 60)


if __name__ == "__main__":
    main()
