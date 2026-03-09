#!/usr/bin/env python3
from __future__ import annotations

import csv
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from neo4j import GraphDatabase


# ----------------------------
# Configuration
# ----------------------------
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "Teeroyce@07")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE", "neo4j")

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
    value_lower = value.lower()
    if value_lower in {"true", "t", "1", "yes", "y"}:
        return True
    if value_lower in {"false", "f", "0", "no", "n"}:
        return False
    return None


def chunked(iterable: Iterable[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
    batch: List[Dict[str, Any]] = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def run_query(driver: GraphDatabase.driver, query: str, **params: Any) -> None:
    with driver.session(database=NEO4J_DATABASE) as session:
        session.run(query, **params).consume()


# ----------------------------
# Reset and schema
# ----------------------------
def reset_database(driver: GraphDatabase.driver) -> None:
    print("Resetting Neo4j database...")

    drop_queries = [
        "DROP CONSTRAINT campaign_key_unique IF EXISTS",
        "DROP CONSTRAINT user_id_unique IF EXISTS",
        "DROP CONSTRAINT product_id_unique IF EXISTS",
        "DROP CONSTRAINT message_id_unique IF EXISTS",
        "DROP CONSTRAINT client_id_unique IF EXISTS",
    ]
    for q in drop_queries:
        run_query(driver, q)

    run_query(driver, "MATCH (n) DETACH DELETE n")

    create_queries = [
        """
        CREATE CONSTRAINT campaign_key_unique IF NOT EXISTS
        FOR (c:Campaign) REQUIRE c.campaign_key IS UNIQUE
        """,
        """
        CREATE CONSTRAINT user_id_unique IF NOT EXISTS
        FOR (u:User) REQUIRE u.user_id IS UNIQUE
        """,
        """
        CREATE CONSTRAINT product_id_unique IF NOT EXISTS
        FOR (p:Product) REQUIRE p.product_id IS UNIQUE
        """,
        """
        CREATE CONSTRAINT message_id_unique IF NOT EXISTS
        FOR (m:Message) REQUIRE m.message_id IS UNIQUE
        """,
        """
        CREATE CONSTRAINT client_id_unique IF NOT EXISTS
        FOR (cl:Client) REQUIRE cl.client_id IS UNIQUE
        """,
    ]
    for q in create_queries:
        run_query(driver, q)

    print("Neo4j schema ready.")


# ----------------------------
# Load campaigns
# ----------------------------
def campaign_rows() -> Iterable[Dict[str, Any]]:
    with CAMPAIGNS_CSV.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            campaign_id = parse_int(row.get("id"))
            campaign_type = clean_str(row.get("campaign_type"))
            if campaign_id is None or campaign_type is None:
                continue

            props = {
                "campaign_id": campaign_id,
                "campaign_type": campaign_type,
                "channel": clean_str(row.get("channel")),
                "topic": clean_str(row.get("topic")),
                "started_at": clean_str(row.get("started_at")),
                "finished_at": clean_str(row.get("finished_at")),
                "total_count": parse_float(row.get("total_count")),
                "ab_test": parse_bool(row.get("ab_test")),
                "warmup_mode": parse_bool(row.get("warmup_mode")),
                "hour_limit": parse_float(row.get("hour_limit")),
                "subject_length": parse_float(row.get("subject_length")),
                "subject_with_personalization": parse_bool(row.get("subject_with_personalization")),
                "subject_with_deadline": parse_bool(row.get("subject_with_deadline")),
                "subject_with_emoji": parse_bool(row.get("subject_with_emoji")),
                "subject_with_bonuses": parse_bool(row.get("subject_with_bonuses")),
                "subject_with_discount": parse_bool(row.get("subject_with_discount")),
                "subject_with_saleout": parse_bool(row.get("subject_with_saleout")),
                "is_test": parse_bool(row.get("is_test")),
                "position": parse_int(row.get("position")),
            }

            yield {
                "campaign_key": f"{campaign_type}_{campaign_id}",
                "props": props,
            }


def load_campaigns(driver: GraphDatabase.driver) -> None:
    print("Loading campaigns...")
    query = """
    UNWIND $rows AS row
    MERGE (c:Campaign {campaign_key: row.campaign_key})
    SET c += row.props
    """
    total = 0
    for batch in chunked(campaign_rows(), BATCH_SIZE):
        run_query(driver, query, rows=batch)
        total += len(batch)
        print(f"  Campaigns loaded: {total}")
    print("Campaigns done.")


# ----------------------------
# Load unique users from events
# ----------------------------
def event_user_rows() -> Iterable[Dict[str, Any]]:
    seen = set()
    with EVENTS_CSV.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            user_id = parse_int(row.get("user_id"))
            if user_id is None or user_id in seen:
                continue
            seen.add(user_id)
            yield {"user_id": user_id}


def load_event_users(driver: GraphDatabase.driver) -> None:
    print("Loading users from events...")
    query = """
    UNWIND $rows AS row
    MERGE (:User {user_id: row.user_id})
    """
    total = 0
    for batch in chunked(event_user_rows(), BATCH_SIZE):
        run_query(driver, query, rows=batch)
        total += len(batch)
        print(f"  Users loaded: {total}")
    print("Users from events done.")


# ----------------------------
# Load unique products from events
# ----------------------------
def event_product_rows() -> Iterable[Dict[str, Any]]:
    seen = set()
    with EVENTS_CSV.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            product_id = parse_int(row.get("product_id"))
            if product_id is None or product_id in seen:
                continue
            seen.add(product_id)
            yield {"product_id": product_id}


def load_event_products(driver: GraphDatabase.driver) -> None:
    print("Loading products from events...")
    query = """
    UNWIND $rows AS row
    MERGE (:Product {product_id: row.product_id})
    """
    total = 0
    for batch in chunked(event_product_rows(), BATCH_SIZE):
        run_query(driver, query, rows=batch)
        total += len(batch)
        print(f"  Products loaded: {total}")
    print("Products from events done.")


# ----------------------------
# Load interactions from events
# ----------------------------
def event_interaction_rows() -> Iterable[Dict[str, Any]]:
    with EVENTS_CSV.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            user_id = parse_int(row.get("user_id"))
            product_id = parse_int(row.get("product_id"))
            if user_id is None or product_id is None:
                continue

            yield {
                "user_id": user_id,
                "product_id": product_id,
                "event_id": parse_int(row.get("event_id")),
                "event_time": clean_str(row.get("event_time")),
                "event_type": clean_str(row.get("event_type")),
                "user_session": clean_str(row.get("user_session")),
                "price": parse_float(row.get("price")),
            }


def load_event_interactions(driver: GraphDatabase.driver) -> None:
    print("Loading interactions from events...")
    query = """
    UNWIND $rows AS row
    MATCH (u:User {user_id: row.user_id})
    MATCH (p:Product {product_id: row.product_id})
    CREATE (u)-[:INTERACTED {
        event_id: row.event_id,
        event_time: row.event_time,
        event_type: row.event_type,
        user_session: row.user_session,
        price: row.price
    }]->(p)
    """
    total = 0
    for batch in chunked(event_interaction_rows(), BATCH_SIZE):
        run_query(driver, query, rows=batch)
        total += len(batch)
        print(f"  INTERACTED relationships loaded: {total}")
    print("Interactions done.")


# ----------------------------
# Load friends
# ----------------------------
def friend_rows() -> Iterable[Dict[str, Any]]:
    with FRIENDS_CSV.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            friend1 = parse_int(row.get("friend1"))
            friend2 = parse_int(row.get("friend2"))
            if friend1 is None or friend2 is None:
                continue
            yield {"friend1": friend1, "friend2": friend2}


def load_friends(driver: GraphDatabase.driver) -> None:
    print("Loading friendships...")
    query = """
    UNWIND $rows AS row
    MERGE (u1:User {user_id: row.friend1})
    MERGE (u2:User {user_id: row.friend2})
    MERGE (u1)-[:FRIEND_WITH]->(u2)
    """
    total = 0
    for batch in chunked(friend_rows(), BATCH_SIZE):
        run_query(driver, query, rows=batch)
        total += len(batch)
        print(f"  FRIEND_WITH relationships loaded: {total}")
    print("Friendships done.")


# ----------------------------
# Load messages
# ----------------------------
def message_rows() -> Iterable[Dict[str, Any]]:
    with MESSAGES_CSV.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            message_id = clean_str(row.get("message_id"))
            campaign_id = parse_int(row.get("campaign_id"))
            message_type = clean_str(row.get("message_type"))

            if message_id is None:
                continue

            campaign_key = None
            if campaign_id is not None and message_type is not None:
                campaign_key = f"{message_type}_{campaign_id}"

            yield {
                "message_id": message_id,
                "campaign_key": campaign_key,
                "campaign_id": campaign_id,
                "message_type": message_type,
                "channel": clean_str(row.get("channel")),
                "client_id": clean_str(row.get("client_id")),
                "sent_at": clean_str(row.get("sent_at")),
                "is_opened": parse_bool(row.get("is_opened")),
                "is_clicked": parse_bool(row.get("is_clicked")),
                "is_purchased": parse_bool(row.get("is_purchased")),
                "user_id": parse_int(row.get("user_id")),
            }


def load_messages(driver: GraphDatabase.driver) -> None:
    print("Loading messages...")
    query = """
    UNWIND $rows AS row
    MERGE (m:Message {message_id: row.message_id})
    SET m.campaign_id = row.campaign_id,
        m.message_type = row.message_type,
        m.channel = row.channel,
        m.sent_at = row.sent_at,
        m.is_opened = row.is_opened,
        m.is_clicked = row.is_clicked,
        m.is_purchased = row.is_purchased

    FOREACH (_ IN CASE WHEN row.campaign_key IS NOT NULL THEN [1] ELSE [] END |
        MERGE (c:Campaign {campaign_key: row.campaign_key})
        ON CREATE SET
            c.campaign_id = row.campaign_id,
            c.campaign_type = row.message_type
        MERGE (m)-[:PART_OF]->(c)
    )

    FOREACH (_ IN CASE WHEN row.client_id IS NOT NULL THEN [1] ELSE [] END |
        MERGE (cl:Client {client_id: row.client_id})
        MERGE (cl)-[:RECEIVED]->(m)
    )

    FOREACH (_ IN CASE WHEN row.user_id IS NOT NULL THEN [1] ELSE [] END |
        MERGE (u:User {user_id: row.user_id})
        MERGE (u)-[:SENT]->(m)
    )
    """
    total = 0
    for batch in chunked(message_rows(), BATCH_SIZE):
        run_query(driver, query, rows=batch)
        total += len(batch)
        print(f"  Messages loaded: {total}")
    print("Messages done.")


# ----------------------------
# Load client first purchase
# ----------------------------
def client_purchase_rows() -> Iterable[Dict[str, Any]]:
    with CLIENT_FIRST_PURCHASE_CSV.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            client_id = clean_str(row.get("client_id"))
            if client_id is None:
                continue
            yield {
                "client_id": client_id,
                "first_purchase_date": clean_str(row.get("first_purchase_date")),
            }


def load_client_first_purchase(driver: GraphDatabase.driver) -> None:
    print("Loading client first purchase dates...")
    query = """
    UNWIND $rows AS row
    MERGE (cl:Client {client_id: row.client_id})
    SET cl.first_purchase_date = row.first_purchase_date
    """
    total = 0
    for batch in chunked(client_purchase_rows(), BATCH_SIZE):
        run_query(driver, query, rows=batch)
        total += len(batch)
        print(f"  Client first purchase rows loaded: {total}")
    print("Client first purchase dates done.")


# ----------------------------
# Verify counts
# ----------------------------
def print_counts(driver: GraphDatabase.driver) -> None:
    checks = [
        ("Campaigns", "MATCH (c:Campaign) RETURN count(c) AS n"),
        ("Users", "MATCH (u:User) RETURN count(u) AS n"),
        ("Products", "MATCH (p:Product) RETURN count(p) AS n"),
        ("Messages", "MATCH (m:Message) RETURN count(m) AS n"),
        ("Clients", "MATCH (cl:Client) RETURN count(cl) AS n"),
        ("INTERACTED", "MATCH ()-[r:INTERACTED]->() RETURN count(r) AS n"),
        ("FRIEND_WITH", "MATCH ()-[r:FRIEND_WITH]->() RETURN count(r) AS n"),
        ("SENT", "MATCH ()-[r:SENT]->() RETURN count(r) AS n"),
        ("PART_OF", "MATCH ()-[r:PART_OF]->() RETURN count(r) AS n"),
        ("RECEIVED", "MATCH ()-[r:RECEIVED]->() RETURN count(r) AS n"),
    ]

    with driver.session(database=NEO4J_DATABASE) as session:
        print("\nFinal Neo4j counts:")
        for label, query in checks:
            result = session.run(query).single()
            print(f"  {label}: {result['n']}")


# ----------------------------
# Main
# ----------------------------
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

    driver = GraphDatabase.driver(
        NEO4J_URI,
        auth=(NEO4J_USER, NEO4J_PASSWORD),
    )

    try:
        reset_database(driver)
        load_campaigns(driver)
        load_event_users(driver)
        load_event_products(driver)
        load_event_interactions(driver)
        load_friends(driver)
        load_messages(driver)
        load_client_first_purchase(driver)
        print_counts(driver)
        print("\nNeo4j graph loading completed successfully.")
    finally:
        driver.close()


if __name__ == "__main__":
    main()
