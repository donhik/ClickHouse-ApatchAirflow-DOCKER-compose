-- Migration 001: Initial schema setup
-- Applied: 2026-02-05 23:56:05

CREATE DATABASE IF NOT EXISTS analytics;
CREATE DATABASE IF NOT EXISTS airflow_metrics;
CREATE DATABASE IF NOT EXISTS logs;

USE analytics;

CREATE TABLE IF NOT EXISTS events (
    event_id UUID DEFAULT generateUUIDv4(),
    user_id UInt32,
    event_type String,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (created_at, user_id);

CREATE TABLE IF NOT EXISTS migration_history (
    migration_id String,
    applied_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY applied_at;

INSERT INTO migration_history (migration_id) VALUES ('001');
