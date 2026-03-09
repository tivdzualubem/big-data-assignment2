#!/usr/bin/env python3
from __future__ import annotations

import csv
import os
from pathlib import Path
from typing import Iterable, List, Tuple, Optional

import psycopg2
from psycopg2.extras import execute_batch


# ----------------------------
# Configuration
# ----------------------------
PG_HOST = os.getenv("PGHOST", "localhost")
PG_PORT = os.getenv("PGPORT", "5432")
PG_USER = os.getenv("PGUSER", "postgres")
PG_PASSWORD = os.getenv("PGPASSWORD", "postgres")
PG_DATABASE = os.getenv("PGDATABASE", "ecommerce_custom")

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data_clean"

CAMPAIGNS_CSV = DATA_DIR / "Campaigns.csv"
EVENTS_CSV = DATA_DIR / "Events.csv"
FRIENDS_CSV = DATA_DIR / "Friends.csv"
MESSAGES_CSV = DATA_DIR / "Messages.csv"
CLIENT_FIRST_PURCHASE_CSV = DATA_DIR / "ClientFirstPurchaseDate.csv"

BATCH_SIZE = 5000


# ----------------------------
# Helpers
# ----------------------------
def clean_str(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    value = value.strip()
    return value if value != "" else None


def parse_int(value: Optional[str]) -> Optional[int]:
    value = clean_str(value)
    if value is None:
        return None
    try:
        return int(float(value))
    except Exception:
        return None


def parse_float(value: Optional[str]) -> Optional[float]:
    value = clean_str(value)
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def parse_bool(value: Optional[str]) -> Optional[bool]:
    value = clean_str(value)
    if value is None:
        return None
    value = value.lower()
    if value in {"true", "t", "1", "yes", "y"}:
        return True
    if value in {"false", "f", "0", "no", "n"}:
        return False
    return None


def chunked_rows(reader: Iterable[Tuple], size: int) -> Iterable[List[Tuple]]:
    batch: List[Tuple] = []
    for row in reader:
        batch.append(row)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


# ----------------------------
# DB setup
# ----------------------------
def connect_postgres(database: str):
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=database,
    )


def create_database_if_not_exists() -> None:
    conn = connect_postgres("postgres")
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (PG_DATABASE,))
    exists = cur.fetchone()

    if not exists:
        cur.execute(f'CREATE DATABASE "{PG_DATABASE}"')
        print(f"Created database: {PG_DATABASE}")
    else:
        print(f"Database already exists: {PG_DATABASE}")

    cur.close()
    conn.close()


def reset_schema(conn) -> None:
    cur = conn.cursor()

    cur.execute("""
    DROP TABLE IF EXISTS fact_messages CASCADE;
    DROP TABLE IF EXISTS fact_events CASCADE;
    DROP TABLE IF EXISTS fact_friendships CASCADE;
    DROP TABLE IF EXISTS dim_clients CASCADE;
    DROP TABLE IF EXISTS dim_campaigns CASCADE;
    DROP TABLE IF EXISTS dim_products CASCADE;
    DROP TABLE IF EXISTS dim_users CASCADE;
    """)

    cur.execute("""
    CREATE TABLE dim_users (
        user_id BIGINT PRIMARY KEY
    );
    """)

    cur.execute("""
    CREATE TABLE dim_products (
        product_id BIGINT PRIMARY KEY
    );
    """)

    cur.execute("""
    CREATE TABLE dim_campaigns (
        campaign_key TEXT PRIMARY KEY,
        campaign_id BIGINT,
        campaign_type TEXT,
        channel TEXT,
        topic TEXT,
        started_at TIMESTAMP NULL,
        finished_at TIMESTAMP NULL,
        total_count DOUBLE PRECISION NULL,
        ab_test BOOLEAN NULL,
        warmup_mode BOOLEAN NULL,
        hour_limit DOUBLE PRECISION NULL,
        subject_length DOUBLE PRECISION NULL,
        subject_with_personalization BOOLEAN NULL,
        subject_with_deadline BOOLEAN NULL,
        subject_with_emoji BOOLEAN NULL,
        subject_with_bonuses BOOLEAN NULL,
        subject_with_discount BOOLEAN NULL,
        subject_with_saleout BOOLEAN NULL,
        is_test BOOLEAN NULL,
        position INTEGER NULL
    );
    """)

    cur.execute("""
    CREATE TABLE dim_clients (
        client_id TEXT PRIMARY KEY,
        first_purchase_date DATE NULL
    );
    """)

    cur.execute("""
    CREATE TABLE fact_events (
        event_id BIGINT PRIMARY KEY,
        event_time TIMESTAMPTZ,
        event_type TEXT,
        product_id BIGINT,
        user_id BIGINT,
        user_session TEXT,
        price DOUBLE PRECISION
    );
    """)

    cur.execute("""
    CREATE TABLE fact_messages (
        message_id TEXT PRIMARY KEY,
        campaign_key TEXT,
        campaign_id BIGINT,
        message_type TEXT,
        channel TEXT,
        client_id TEXT,
        sent_at TIMESTAMP NULL,
        is_opened BOOLEAN NULL,
        is_clicked BOOLEAN NULL,
        is_purchased BOOLEAN NULL,
        user_id BIGINT NULL
    );
    """)

    cur.execute("""
    CREATE TABLE fact_friendships (
        friendship_id BIGSERIAL PRIMARY KEY,
        friend1 BIGINT,
        friend2 BIGINT
    );
    """)

    cur.execute("""
    CREATE INDEX idx_fact_events_user_id ON fact_events(user_id);
    CREATE INDEX idx_fact_events_product_id ON fact_events(product_id);
    CREATE INDEX idx_fact_events_event_type ON fact_events(event_type);

    CREATE INDEX idx_fact_messages_campaign_key ON fact_messages(campaign_key);
    CREATE INDEX idx_fact_messages_client_id ON fact_messages(client_id);
    CREATE INDEX idx_fact_messages_user_id ON fact_messages(user_id);

    CREATE INDEX idx_fact_friendships_friend1 ON fact_friendships(friend1);
    CREATE INDEX idx_fact_friendships_friend2 ON fact_friendships(friend2);
    """)

    conn.commit()
    cur.close()
    print("Custom schema created successfully.")


# ----------------------------
# Load dimensions
# ----------------------------
def load_dim_campaigns(conn) -> None:
    print("Loading dim_campaigns...")
    sql = """
    INSERT INTO dim_campaigns (
        campaign_key, campaign_id, campaign_type, channel, topic,
        started_at, finished_at, total_count, ab_test, warmup_mode,
        hour_limit, subject_length, subject_with_personalization,
        subject_with_deadline, subject_with_emoji, subject_with_bonuses,
        subject_with_discount, subject_with_saleout, is_test, position
    )
    VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    ON CONFLICT (campaign_key) DO NOTHING;
    """

    with CAMPAIGNS_CSV.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        cur = conn.cursor()
        total = 0

        def rows():
            for row in reader:
                campaign_id = parse_int(row.get("id"))
                campaign_type = clean_str(row.get("campaign_type"))
                if campaign_id is None or campaign_type is None:
                    continue

                yield (
                    f"{campaign_type}_{campaign_id}",
                    campaign_id,
                    campaign_type,
                    clean_str(row.get("channel")),
                    clean_str(row.get("topic")),
                    clean_str(row.get("started_at")),
                    clean_str(row.get("finished_at")),
                    parse_float(row.get("total_count")),
                    parse_bool(row.get("ab_test")),
                    parse_bool(row.get("warmup_mode")),
                    parse_float(row.get("hour_limit")),
                    parse_float(row.get("subject_length")),
                    parse_bool(row.get("subject_with_personalization")),
                    parse_bool(row.get("subject_with_deadline")),
                    parse_bool(row.get("subject_with_emoji")),
                    parse_bool(row.get("subject_with_bonuses")),
                    parse_bool(row.get("subject_with_discount")),
                    parse_bool(row.get("subject_with_saleout")),
                    parse_bool(row.get("is_test")),
                    parse_int(row.get("position")),
                )

        for batch in chunked_rows(rows(), BATCH_SIZE):
            execute_batch(cur, sql, batch, page_size=BATCH_SIZE)
            conn.commit()
            total += len(batch)
            print(f"  dim_campaigns loaded: {total}")

        cur.close()


def load_dim_users_from_events(conn) -> None:
    print("Loading dim_users from Events.csv...")
    sql = """
    INSERT INTO dim_users (user_id)
    VALUES (%s)
    ON CONFLICT (user_id) DO NOTHING;
    """

    with EVENTS_CSV.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        cur = conn.cursor()
        total = 0

        def rows():
            for row in reader:
                user_id = parse_int(row.get("user_id"))
                if user_id is not None:
                    yield (user_id,)

        for batch in chunked_rows(rows(), BATCH_SIZE):
            execute_batch(cur, sql, batch, page_size=BATCH_SIZE)
            conn.commit()
            total += len(batch)
            print(f"  dim_users rows processed: {total}")

        cur.close()


def load_dim_products_from_events(conn) -> None:
    print("Loading dim_products from Events.csv...")
    sql = """
    INSERT INTO dim_products (product_id)
    VALUES (%s)
    ON CONFLICT (product_id) DO NOTHING;
    """

    with EVENTS_CSV.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        cur = conn.cursor()
        total = 0

        def rows():
            for row in reader:
                product_id = parse_int(row.get("product_id"))
                if product_id is not None:
                    yield (product_id,)

        for batch in chunked_rows(rows(), BATCH_SIZE):
            execute_batch(cur, sql, batch, page_size=BATCH_SIZE)
            conn.commit()
            total += len(batch)
            print(f"  dim_products rows processed: {total}")

        cur.close()


def load_dim_users_from_messages(conn) -> None:
    print("Loading extra dim_users from Messages.csv...")
    sql = """
    INSERT INTO dim_users (user_id)
    VALUES (%s)
    ON CONFLICT (user_id) DO NOTHING;
    """

    with MESSAGES_CSV.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        cur = conn.cursor()
        total = 0

        def rows():
            for row in reader:
                user_id = parse_int(row.get("user_id"))
                if user_id is not None:
                    yield (user_id,)

        for batch in chunked_rows(rows(), BATCH_SIZE):
            execute_batch(cur, sql, batch, page_size=BATCH_SIZE)
            conn.commit()
            total += len(batch)
            print(f"  dim_users from messages processed: {total}")

        cur.close()


def load_dim_clients(conn) -> None:
    print("Loading dim_clients...")
    sql = """
    INSERT INTO dim_clients (client_id, first_purchase_date)
    VALUES (%s, %s)
    ON CONFLICT (client_id) DO UPDATE
    SET first_purchase_date = EXCLUDED.first_purchase_date;
    """

    with CLIENT_FIRST_PURCHASE_CSV.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        cur = conn.cursor()
        total = 0

        def rows():
            for row in reader:
                client_id = clean_str(row.get("client_id"))
                if client_id is None:
                    continue
                yield (
                    client_id,
                    clean_str(row.get("first_purchase_date")),
                )

        for batch in chunked_rows(rows(), BATCH_SIZE):
            execute_batch(cur, sql, batch, page_size=BATCH_SIZE)
            conn.commit()
            total += len(batch)
            print(f"  dim_clients loaded: {total}")

        cur.close()


# ----------------------------
# Load facts
# ----------------------------
def load_fact_events(conn) -> None:
    print("Loading fact_events...")
    sql = """
    INSERT INTO fact_events (
        event_id, event_time, event_type, product_id, user_id, user_session, price
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (event_id) DO NOTHING;
    """

    with EVENTS_CSV.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        cur = conn.cursor()
        total = 0

        def rows():
            for row in reader:
                event_id = parse_int(row.get("event_id"))
                if event_id is None:
                    continue
                yield (
                    event_id,
                    clean_str(row.get("event_time")),
                    clean_str(row.get("event_type")),
                    parse_int(row.get("product_id")),
                    parse_int(row.get("user_id")),
                    clean_str(row.get("user_session")),
                    parse_float(row.get("price")),
                )

        for batch in chunked_rows(rows(), BATCH_SIZE):
            execute_batch(cur, sql, batch, page_size=BATCH_SIZE)
            conn.commit()
            total += len(batch)
            print(f"  fact_events loaded: {total}")

        cur.close()


def load_fact_messages(conn) -> None:
    print("Loading fact_messages...")
    sql = """
    INSERT INTO fact_messages (
        message_id, campaign_key, campaign_id, message_type, channel,
        client_id, sent_at, is_opened, is_clicked, is_purchased, user_id
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (message_id) DO NOTHING;
    """

    with MESSAGES_CSV.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        cur = conn.cursor()
        total = 0

        def rows():
            for row in reader:
                message_id = clean_str(row.get("message_id"))
                campaign_id = parse_int(row.get("campaign_id"))
                message_type = clean_str(row.get("message_type"))
                if message_id is None:
                    continue

                campaign_key = None
                if campaign_id is not None and message_type is not None:
                    campaign_key = f"{message_type}_{campaign_id}"

                yield (
                    message_id,
                    campaign_key,
                    campaign_id,
                    message_type,
                    clean_str(row.get("channel")),
                    clean_str(row.get("client_id")),
                    clean_str(row.get("sent_at")),
                    parse_bool(row.get("is_opened")),
                    parse_bool(row.get("is_clicked")),
                    parse_bool(row.get("is_purchased")),
                    parse_int(row.get("user_id")),
                )

        for batch in chunked_rows(rows(), BATCH_SIZE):
            execute_batch(cur, sql, batch, page_size=BATCH_SIZE)
            conn.commit()
            total += len(batch)
            print(f"  fact_messages loaded: {total}")

        cur.close()


def load_fact_friendships(conn) -> None:
    print("Loading fact_friendships...")
    sql = """
    INSERT INTO fact_friendships (friend1, friend2)
    VALUES (%s, %s);
    """

    with FRIENDS_CSV.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        cur = conn.cursor()
        total = 0

        def rows():
            for row in reader:
                friend1 = parse_int(row.get("friend1"))
                friend2 = parse_int(row.get("friend2"))
                if friend1 is None or friend2 is None:
                    continue
                yield (friend1, friend2)

        for batch in chunked_rows(rows(), BATCH_SIZE):
            execute_batch(cur, sql, batch, page_size=BATCH_SIZE)
            conn.commit()
            total += len(batch)
            print(f"  fact_friendships loaded: {total}")

        cur.close()


# ----------------------------
# Verification
# ----------------------------
def print_counts(conn) -> None:
    cur = conn.cursor()
    checks = [
        ("dim_users", "SELECT COUNT(*) FROM dim_users"),
        ("dim_products", "SELECT COUNT(*) FROM dim_products"),
        ("dim_campaigns", "SELECT COUNT(*) FROM dim_campaigns"),
        ("dim_clients", "SELECT COUNT(*) FROM dim_clients"),
        ("fact_events", "SELECT COUNT(*) FROM fact_events"),
        ("fact_messages", "SELECT COUNT(*) FROM fact_messages"),
        ("fact_friendships", "SELECT COUNT(*) FROM fact_friendships"),
    ]

    print("\nFinal custom model counts:")
    for name, query in checks:
        cur.execute(query)
        value = cur.fetchone()[0]
        print(f"  {name}: {value}")

    cur.close()


def main() -> None:
    required_files = [
        CAMPAIGNS_CSV,
        EVENTS_CSV,
        FRIENDS_CSV,
        MESSAGES_CSV,
        CLIENT_FIRST_PURCHASE_CSV,
    ]
    for path in required_files:
        if not path.exists():
            raise FileNotFoundError(f"Missing required file: {path}")

    create_database_if_not_exists()
    conn = connect_postgres(PG_DATABASE)

    try:
        reset_schema(conn)

        load_dim_campaigns(conn)
        load_dim_users_from_events(conn)
        load_dim_products_from_events(conn)
        load_dim_users_from_messages(conn)
        load_dim_clients(conn)

        load_fact_events(conn)
        load_fact_messages(conn)
        load_fact_friendships(conn)

        print_counts(conn)
        print("\nCustom scalable model loaded successfully.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
