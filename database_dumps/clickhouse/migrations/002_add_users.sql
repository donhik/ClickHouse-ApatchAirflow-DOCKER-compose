-- Migration 002: Add users table
-- Applied: 2026-02-05 23:56:06

USE analytics;

CREATE TABLE IF NOT EXISTS users (
    user_id UInt32,
    email String,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY user_id;

INSERT INTO migration_history (migration_id) VALUES ('002');
