#!/usr/bin/env python3
"""
Insert demo rows into MySQL so students can watch the Python pipeline move data.
"""

import argparse
from datetime import datetime, timezone

import mysql.connector


MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3307,
    "user": "kafka",
    "password": "kafka123",
    "database": "kafka_course",
}


def main():
    parser = argparse.ArgumentParser(description="Insert one customer and one order into MySQL.")
    parser.add_argument("--name", default="Python Student")
    parser.add_argument("--country", default="USA")
    parser.add_argument("--product", default="Kafka Python Lab")
    parser.add_argument("--amount", type=float, default=79.0)
    args = parser.parse_args()

    suffix = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    email = f"student-{suffix}@example.com"

    connection = mysql.connector.connect(**MYSQL_CONFIG)
    try:
        cursor = connection.cursor()
        cursor.execute(
            "INSERT INTO customers (name, email, country) VALUES (%s, %s, %s)",
            (args.name, email, args.country),
        )
        customer_id = cursor.lastrowid
        cursor.execute(
            "INSERT INTO orders (customer_id, product, amount, status) VALUES (%s, %s, %s, %s)",
            (customer_id, args.product, args.amount, "completed"),
        )
        order_id = cursor.lastrowid
        connection.commit()
    finally:
        connection.close()

    print(f"inserted customer id={customer_id}, order id={order_id}")


if __name__ == "__main__":
    main()
