import os
import gc
import csv
import pandas as pd
import numpy as np

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(BASE_DIR, "data_raw")
CLEAN_DIR = os.path.join(BASE_DIR, "data_clean")
os.makedirs(CLEAN_DIR, exist_ok=True)

CHUNK_SIZE = 50000


def parse_bool(value):
    if pd.isna(value):
        return False
    s = str(value).strip().lower()
    return s in {"true", "1", "t", "yes"}


campaigns_path = os.path.join(RAW_DIR, "campaigns.csv")
first_purchase_path = os.path.join(RAW_DIR, "client_first_purchase_date.csv")
events_path = os.path.join(RAW_DIR, "events.csv")
friends_path = os.path.join(RAW_DIR, "friends.csv")
messages_path = os.path.join(RAW_DIR, "messages.csv")

# -----------------------------
# Small files
# -----------------------------
campaigns = pd.read_csv(campaigns_path, low_memory=False)
campaigns["started_at"] = pd.to_datetime(campaigns["started_at"], errors="coerce")
campaigns["finished_at"] = pd.to_datetime(campaigns["finished_at"], errors="coerce")

campaigns_out = campaigns[[
    "id", "campaign_type", "channel", "topic", "started_at", "finished_at",
    "total_count", "ab_test", "warmup_mode", "hour_limit", "subject_length",
    "subject_with_personalization", "subject_with_deadline", "subject_with_emoji",
    "subject_with_bonuses", "subject_with_discount", "subject_with_saleout",
    "is_test", "position"
]].drop_duplicates()
campaigns_out.to_csv(os.path.join(CLEAN_DIR, "Campaigns.csv"), index=False)

del campaigns, campaigns_out
gc.collect()

first_purchase = pd.read_csv(first_purchase_path, low_memory=False)
first_purchase["first_purchase_date"] = pd.to_datetime(first_purchase["first_purchase_date"], errors="coerce")
first_purchase_out = first_purchase[["client_id", "first_purchase_date"]].drop_duplicates()
first_purchase_out.to_csv(os.path.join(CLEAN_DIR, "ClientFirstPurchaseDate.csv"), index=False)

del first_purchase, first_purchase_out
gc.collect()

friends = pd.read_csv(friends_path, low_memory=False)
friends[["friend1", "friend2"]] = friends[["friend1", "friend2"]].apply(pd.to_numeric, errors="coerce")
friends[["friend1", "friend2"]] = np.sort(friends[["friend1", "friend2"]], axis=1)
friends_out = friends[["friend1", "friend2"]].dropna().drop_duplicates()
friends_out.to_csv(os.path.join(CLEAN_DIR, "Friends.csv"), index=False)

del friends, friends_out
gc.collect()

# -----------------------------
# Users from small files first
# -----------------------------
users_set = set()

# from first_purchase, only if user_id exists in that file
first_purchase_cols = pd.read_csv(first_purchase_path, nrows=0).columns.tolist()
if "user_id" in first_purchase_cols:
    first_purchase_users = pd.read_csv(first_purchase_path, usecols=["user_id"], low_memory=False)
    first_purchase_users["user_id"] = pd.to_numeric(first_purchase_users["user_id"], errors="coerce")
    for u in first_purchase_users["user_id"].dropna().astype(int).tolist():
        users_set.add(u)
    del first_purchase_users
    gc.collect()

# from friends
friends_users = pd.read_csv(friends_path, usecols=["friend1", "friend2"], low_memory=False)
friends_users = friends_users.apply(pd.to_numeric, errors="coerce")
for col in ["friend1", "friend2"]:
    for u in friends_users[col].dropna().astype(int).tolist():
        users_set.add(u)
del friends_users
gc.collect()

# -----------------------------
# Events in chunks
# -----------------------------
events_out_path = os.path.join(CLEAN_DIR, "Events.csv")
products_out_path = os.path.join(CLEAN_DIR, "Products.csv")

first_events_chunk = True
event_id_counter = 1
products_seen = set()

# prepare products csv header once
with open(products_out_path, "w", newline="") as pf:
    writer = csv.writer(pf)
    writer.writerow(["product_id", "category_id", "category_code", "brand", "price"])

for chunk in pd.read_csv(events_path, chunksize=CHUNK_SIZE, low_memory=False):
    chunk["event_time"] = pd.to_datetime(chunk["event_time"], errors="coerce")
    chunk["product_id"] = pd.to_numeric(chunk["product_id"], errors="coerce")
    chunk["category_id"] = pd.to_numeric(chunk["category_id"], errors="coerce")
    chunk["price"] = pd.to_numeric(chunk["price"], errors="coerce")
    chunk["user_id"] = pd.to_numeric(chunk["user_id"], errors="coerce")

    # users from events
    for u in chunk["user_id"].dropna().astype(int).unique():
        users_set.add(int(u))

    # write events
    events_out = chunk[["event_time", "event_type", "product_id", "user_id", "user_session", "price"]].copy()
    events_out.insert(0, "event_id", range(event_id_counter, event_id_counter + len(events_out)))
    event_id_counter += len(events_out)
    events_out.to_csv(events_out_path, mode="w" if first_events_chunk else "a", header=first_events_chunk, index=False)
    first_events_chunk = False

    # append unique products incrementally
    prod_chunk = chunk[["product_id", "category_id", "category_code", "brand", "price"]].dropna(subset=["product_id"])
    with open(products_out_path, "a", newline="") as pf:
        writer = csv.writer(pf)
        for row in prod_chunk.itertuples(index=False):
            pid = int(row.product_id)
            if pid not in products_seen:
                products_seen.add(pid)
                writer.writerow([
                    pid,
                    "" if pd.isna(row.category_id) else int(row.category_id),
                    "" if pd.isna(row.category_code) else row.category_code,
                    "" if pd.isna(row.brand) else row.brand,
                    "" if pd.isna(row.price) else float(row.price)
                ])

    del chunk, events_out, prod_chunk
    gc.collect()

# -----------------------------
# Messages in chunks
# -----------------------------
messages_out_path = os.path.join(CLEAN_DIR, "Messages.csv")
first_messages_chunk = True

for chunk in pd.read_csv(messages_path, chunksize=CHUNK_SIZE, low_memory=False):
    keep_cols = [
        "message_id", "campaign_id", "message_type", "channel", "client_id",
        "sent_at", "is_opened", "is_clicked", "is_purchased", "user_id"
    ]
    msg = chunk[keep_cols].copy()

    msg["sent_at"] = pd.to_datetime(msg["sent_at"], errors="coerce")
    msg["campaign_id"] = pd.to_numeric(msg["campaign_id"], errors="coerce")
    msg["client_id"] = pd.to_numeric(msg["client_id"], errors="coerce")
    msg["user_id"] = pd.to_numeric(msg["user_id"], errors="coerce")

    msg["is_opened"] = msg["is_opened"].apply(parse_bool)
    msg["is_clicked"] = msg["is_clicked"].apply(parse_bool)
    msg["is_purchased"] = msg["is_purchased"].apply(parse_bool)

    msg.to_csv(
        messages_out_path,
        mode="w" if first_messages_chunk else "a",
        header=first_messages_chunk,
        index=False
    )
    first_messages_chunk = False

    # users from messages
    for u in msg["user_id"].dropna().astype(int).unique():
        users_set.add(int(u))

    del chunk, msg
    gc.collect()

# -----------------------------
# Final Users.csv
# -----------------------------
users_out = pd.DataFrame({"user_id": sorted(users_set)})
users_out.to_csv(os.path.join(CLEAN_DIR, "Users.csv"), index=False)

print("Cleaning complete. CSV files written to data_clean/")
