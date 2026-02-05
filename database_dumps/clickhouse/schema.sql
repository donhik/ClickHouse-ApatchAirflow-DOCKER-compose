-- ============================================================================
-- CLICKHOUSE DATABASE SCHEMA
-- Project: Airflow + ClickHouse Analytics Platform
-- Version: 1.0
-- Created: 2026-02-05 23:55:48
-- ============================================================================

-- ============================================================================
-- 1. CREATE DATABASES
-- ============================================================================

CREATE DATABASE IF NOT EXISTS analytics;
CREATE DATABASE IF NOT EXISTS airflow_metrics;
CREATE DATABASE IF NOT EXISTS logs;
CREATE DATABASE IF NOT EXISTS staging;

-- ============================================================================
-- 2. ANALYTICS DATABASE
-- ============================================================================

USE analytics;

-- 2.1 Events table
CREATE TABLE IF NOT EXISTS events (
    event_id UUID DEFAULT generateUUIDv4(),
    user_id UInt32,
    session_id String,
    event_type String,
    event_data String,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (created_at, user_id)
PARTITION BY toYYYYMM(created_at);

-- 2.2 Users table
CREATE TABLE IF NOT EXISTS users (
    user_id UInt32,
    email String,
    username String,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY user_id;

-- 2.3 Sessions table
CREATE TABLE IF NOT EXISTS sessions (
    session_id String,
    user_id UInt32,
    started_at DateTime,
    ended_at Nullable(DateTime),
    page_count UInt16 DEFAULT 0,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (started_at, user_id)
PARTITION BY toYYYYMM(started_at);

-- ============================================================================
-- 3. AIRFLOW METRICS DATABASE
-- ============================================================================

USE airflow_metrics;

-- 3.1 DAG execution history
CREATE TABLE IF NOT EXISTS dag_runs (
    dag_id String,
    run_id String,
    execution_date DateTime,
    state String,
    start_date DateTime,
    end_date Nullable(DateTime),
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (dag_id, execution_date)
PARTITION BY toYYYYMM(execution_date);

-- 3.2 Task execution history
CREATE TABLE IF NOT EXISTS task_instances (
    dag_id String,
    task_id String,
    execution_date DateTime,
    state String,
    start_date Nullable(DateTime),
    end_date Nullable(DateTime),
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (dag_id, task_id, execution_date)
PARTITION BY toYYYYMM(execution_date);

-- ============================================================================
-- 4. LOGS DATABASE
-- ============================================================================

USE logs;

-- 4.1 Application logs
CREATE TABLE IF NOT EXISTS app_logs (
    log_id UUID DEFAULT generateUUIDv4(),
    level String,
    message String,
    timestamp DateTime,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY timestamp
PARTITION BY toYYYYMM(timestamp);

-- 4.2 API access logs
CREATE TABLE IF NOT EXISTS api_logs (
    request_id UUID DEFAULT generateUUIDv4(),
    method String,
    endpoint String,
    status_code UInt16,
    response_time_ms UInt32,
    timestamp DateTime,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY timestamp
PARTITION BY toYYYYMM(timestamp);

-- ============================================================================
-- 5. MATERIALIZED VIEWS
-- ============================================================================

USE analytics;

-- 5.1 Daily event statistics
CREATE MATERIALIZED VIEW IF NOT EXISTS daily_event_stats
ENGINE = SummingMergeTree()
ORDER BY (date, event_type)
AS SELECT
    toDate(created_at) AS date,
    event_type,
    count() AS event_count,
    uniq(user_id) AS unique_users
FROM events
GROUP BY date, event_type;

-- 5.2 Daily user statistics
CREATE MATERIALIZED VIEW IF NOT EXISTS daily_user_stats
ENGINE = AggregatingMergeTree()
ORDER BY (date, country)
AS SELECT
    toDate(created_at) AS date,
    'unknown' AS country,  -- placeholder
    count() AS new_users
FROM users
GROUP BY date;

-- ============================================================================
-- 6. SYSTEM TABLES
-- ============================================================================

USE analytics;

-- 6.1 System configuration
CREATE TABLE IF NOT EXISTS system_config (
    config_key String,
    config_value String,
    description String,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY config_key;

-- 6.2 Migration history
CREATE TABLE IF NOT EXISTS migration_history (
    migration_id String,
    migration_name String,
    applied_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY applied_at;

-- ============================================================================
-- 7. INITIAL DATA
-- ============================================================================

-- Insert system configuration
INSERT INTO system_config (config_key, config_value, description) VALUES
    ('schema_version', '1.0', 'Database schema version'),
    ('created_date', '2026-02-05', 'Schema creation date');

-- Record initial migration
INSERT INTO migration_history (migration_id, migration_name) VALUES
    ('001', 'initial_schema_creation');

-- ============================================================================
-- END OF SCHEMA
-- ============================================================================
