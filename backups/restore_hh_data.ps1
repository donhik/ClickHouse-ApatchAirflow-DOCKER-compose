# restore_hh_data.ps1
# Скрипт для восстановления базы hh_data на новом ПК

$ErrorActionPreference = "Stop"

Write-Host "=== ВОССТАНОВЛЕНИЕ БАЗЫ HH_DATA ===" -ForegroundColor Cyan

# Запускаем ClickHouse если не запущен
Write-Host "1. Запуск ClickHouse..." -ForegroundColor Yellow
docker-compose up -d clickhouse-server
Start-Sleep -Seconds 10

# Восстанавливаем базу из дампа
Write-Host "2. Восстановление структуры..." -ForegroundColor Yellow
if (Test-Path "backups\hh_data_dump_20260206_153342.sql") {
    $dumpContent = Get-Content "backups\hh_data_dump_20260206_153342.sql" -Raw
    docker-compose exec -T clickhouse-server clickhouse-client 
        --user admin --password clickhouse_pass 
        --multiquery 
        --query "$dumpContent" 2>restore_errors.log
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✓ База восстановлена!" -ForegroundColor Green
        
        # Проверяем
        $tables = docker-compose exec clickhouse-server clickhouse-client 
            --user admin --password clickhouse_pass 
            --query "SHOW TABLES FROM hh_data" 2>$null
        
        if ($tables) {
            Write-Host "
Таблицы в hh_data:" -ForegroundColor White
            $tables | ForEach-Object { Write-Host "  - $_" -ForegroundColor Gray }
        }
    }
    else {
        Write-Host "✗ Ошибка восстановления!" -ForegroundColor Red
        if (Test-Path "restore_errors.log") {
            Get-Content restore_errors.log
        }
    }
}
else {
    Write-Host "✗ Файл дампа не найден: backups\hh_data_dump_20260206_153342.sql" -ForegroundColor Red
}
