#!/usr/bin/env python3
"""Recreate the sample database using only Python's standard library."""
from pathlib import Path
import sqlite3


def create_db(path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as db:
        db.executescript("""
            DROP TABLE IF EXISTS customers;
            CREATE TABLE customers (
                customer_id TEXT PRIMARY KEY, customer_name TEXT, region TEXT,
                credit_limit INTEGER NOT NULL, score REAL NOT NULL, payload BLOB
            );
        """)
        db.executemany("INSERT INTO customers VALUES (?, ?, ?, ?, ?, ?)", [
            ("0001", "東京商事", "east", 9223372036854775807, 1.5, b"\x00\xff"),
            ("0002", "", "west", -9223372036854775808, 2.25, b""),
            ("0003", None, None, 0, 0.0, None),
        ])
        for table in ["customer_append", "customer_overwrite", "customer_merge"]:
            db.execute(f"DROP TABLE IF EXISTS {table}")
            db.execute(f"CREATE TABLE {table} (customer_id TEXT PRIMARY KEY, customer_name TEXT, region TEXT)")
            db.executemany(f"INSERT INTO {table} VALUES (?, ?, ?)", [
                ("0001", "東京商事", "east"), ("0002", "", "west"), ("0003", None, None),
            ])


if __name__ == "__main__":
    create_db(Path(__file__).parent / "data" / "customers.sqlite")
