# PowerShell script to apply ClickHouse schema
Write-Host "Applying ClickHouse database schema..." -ForegroundColor Cyan

# Main schema
Get-Content database_dumps/clickhouse/schema.sql -Encoding UTF8 | 
    docker-compose exec -T clickhouse clickhouse-client --user admin --password clickhouse_pass

# Migrations
Get-ChildItem database_dumps/clickhouse/migrations/*.sql | Sort-Object Name | ForEach-Object {
    Write-Host "Applying migration: " -ForegroundColor Yellow
    Get-Content  -Encoding UTF8 | 
        docker-compose exec -T clickhouse clickhouse-client --user admin --password clickhouse_pass
}

Write-Host "Schema applied successfully!" -ForegroundColor Green
