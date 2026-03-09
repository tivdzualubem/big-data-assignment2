DROP DATABASE IF EXISTS ecommerce;
CREATE DATABASE ecommerce;
\connect ecommerce;

CREATE SCHEMA IF NOT EXISTS ecommerce;
SET search_path TO ecommerce, public;

CREATE TABLE users (
    user_id BIGINT PRIMARY KEY
);

CREATE TABLE products (
    product_id BIGINT PRIMARY KEY,
    category_id BIGINT,
    category_code VARCHAR,
    brand VARCHAR,
    price NUMERIC
);

CREATE TABLE campaigns (
    id BIGINT NOT NULL,
    campaign_type VARCHAR NOT NULL,
    channel VARCHAR,
    topic VARCHAR,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    total_count NUMERIC,
    ab_test BOOLEAN,
    warmup_mode BOOLEAN,
    hour_limit NUMERIC,
    subject_length NUMERIC,
    subject_with_personalization BOOLEAN,
    subject_with_deadline BOOLEAN,
    subject_with_emoji BOOLEAN,
    subject_with_bonuses BOOLEAN,
    subject_with_discount BOOLEAN,
    subject_with_saleout BOOLEAN,
    is_test BOOLEAN,
    position NUMERIC,
    PRIMARY KEY (id, campaign_type)
);

CREATE TABLE client_first_purchase_date (
    client_id VARCHAR PRIMARY KEY,
    first_purchase_date TIMESTAMP
);

CREATE TABLE friends (
    friend1 BIGINT NOT NULL,
    friend2 BIGINT NOT NULL,
    PRIMARY KEY (friend1, friend2)
);

CREATE TABLE messages (
    message_id VARCHAR PRIMARY KEY,
    campaign_id BIGINT NOT NULL,
    message_type VARCHAR NOT NULL,
    channel VARCHAR,
    client_id VARCHAR,
    sent_at TIMESTAMP,
    is_opened BOOLEAN,
    is_clicked BOOLEAN,
    is_purchased BOOLEAN,
    user_id BIGINT,
    CONSTRAINT fk_messages_campaigns
        FOREIGN KEY (campaign_id, message_type)
        REFERENCES campaigns(id, campaign_type)
);

CREATE TABLE events (
    event_id BIGINT PRIMARY KEY,
    event_time TIMESTAMPTZ,
    event_type VARCHAR,
    product_id BIGINT,
    user_id BIGINT,
    user_session VARCHAR,
    price NUMERIC,
    CONSTRAINT fk_events_products FOREIGN KEY (product_id) REFERENCES products(product_id),
    CONSTRAINT fk_events_users FOREIGN KEY (user_id) REFERENCES users(user_id)
);

\copy ecommerce.users(user_id) FROM '/root/big-data-assignment2/data_clean/Users.csv' CSV HEADER;
\copy ecommerce.products(product_id, category_id, category_code, brand, price) FROM '/root/big-data-assignment2/data_clean/Products.csv' CSV HEADER;
\copy ecommerce.campaigns(id, campaign_type, channel, topic, started_at, finished_at, total_count, ab_test, warmup_mode, hour_limit, subject_length, subject_with_personalization, subject_with_deadline, subject_with_emoji, subject_with_bonuses, subject_with_discount, subject_with_saleout, is_test, position) FROM '/root/big-data-assignment2/data_clean/Campaigns.csv' CSV HEADER;
\copy ecommerce.client_first_purchase_date(client_id, first_purchase_date) FROM '/root/big-data-assignment2/data_clean/ClientFirstPurchaseDate.csv' CSV HEADER;
\copy ecommerce.friends(friend1, friend2) FROM '/root/big-data-assignment2/data_clean/Friends.csv' CSV HEADER;
\copy ecommerce.messages(message_id, campaign_id, message_type, channel, client_id, sent_at, is_opened, is_clicked, is_purchased, user_id) FROM '/root/big-data-assignment2/data_clean/Messages.csv' CSV HEADER;
\copy ecommerce.events(event_id, event_time, event_type, product_id, user_id, user_session, price) FROM '/root/big-data-assignment2/data_clean/Events.csv' CSV HEADER;
