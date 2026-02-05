-- PostgreSQL schema for Apache Airflow
-- This should be run in the PostgreSQL database

-- Airflow metadata tables (already created by Airflow)
-- This file is for documentation purposes

-- Example: Custom table for ClickHouse integration
CREATE TABLE IF NOT EXISTS clickhouse_connections (
    id SERIAL PRIMARY KEY,
    connection_name VARCHAR(255) NOT NULL,
    host VARCHAR(255) NOT NULL,
    port INTEGER DEFAULT 8123,
    database VARCHAR(255) DEFAULT 'default',
    username VARCHAR(255),
    password VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table for tracking data exports
CREATE TABLE IF NOT EXISTS data_exports (
    id SERIAL PRIMARY KEY,
    export_type VARCHAR(100) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    record_count INTEGER,
    status VARCHAR(50) DEFAULT 'pending',
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
