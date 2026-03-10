from pymongo import MongoClient
import psycopg2
from neo4j import GraphDatabase


print("=== MongoDB Analysis ===")

try:
    client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=5000)
    mongo_db = client["ecommerce"]

    collections = mongo_db.list_collection_names()
    print("MongoDB collections:", collections)

    if "campaigns" in collections:
        campaigns_count = mongo_db["campaigns"].count_documents({})
        print("Campaigns (MongoDB):", campaigns_count)
    else:
        print("MongoDB assignment database not loaded yet.")

except Exception as e:
    print("MongoDB error:", e)


print("\n=== PostgreSQL Analysis ===")

try:
    conn = psycopg2.connect(
        dbname="ecommerce_custom",
        user="postgres",
        password="Teeroyce@07",
        host="localhost",
        port="5432"
    )

    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM dim_users;")
    print("dim_users (PostgreSQL):", cursor.fetchone()[0])

    cursor.execute("SELECT COUNT(*) FROM dim_products;")
    print("dim_products (PostgreSQL):", cursor.fetchone()[0])

    cursor.execute("SELECT COUNT(*) FROM fact_events;")
    print("fact_events (PostgreSQL):", cursor.fetchone()[0])

    cursor.close()
    conn.close()

except Exception as e:
    print("PostgreSQL error:", e)


print("\n=== Neo4j Analysis ===")

try:
    driver = GraphDatabase.driver(
        "bolt://localhost:7687",
        auth=("neo4j", "Teeroyce@07")
    )

    with driver.session() as session:

        result = session.run("MATCH (u:User) RETURN count(u) AS c")
        print("Users (Neo4j):", result.single()["c"])

        result = session.run("MATCH (p:Product) RETURN count(p) AS c")
        print("Products (Neo4j):", result.single()["c"])

        result = session.run("MATCH ()-[r:INTERACTED]->() RETURN count(r) AS c")
        print("INTERACTED relationships (Neo4j):", result.single()["c"])

    driver.close()

except Exception as e:
    print("Neo4j error:", e)
