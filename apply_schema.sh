#!/bin/bash
# Script to apply ClickHouse database schema
# Usage: ./apply_schema.sh

CLICKHOUSE_HOST="localhost"
CLICKHOUSE_PORT="8123"
CLICKHOUSE_USER="admin"
CLICKHOUSE_PASSWORD="clickhouse_pass"

echo "Applying ClickHouse schema..."

# Apply main schema
cat database_dumps/clickhouse/schema.sql | \
    clickhouse-client \
    --host \ \
    --port \ \
    --user \ \
    --password \

# Apply migrations
for migration in database_dumps/clickhouse/migrations/*.sql; do
    echo "Applying migration: \"
    cat \ | \
        clickhouse-client \
        --host \ \
        --port \ \
        --user \ \
        --password \
done

echo "Schema applied successfully!"
