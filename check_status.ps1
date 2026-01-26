Write-Host "=== ПРОВЕРКА AIRFLOW + CLICKHOUSE ===" -ForegroundColor Cyan

# Проверка контейнеров
Write-Host "
📦 Проверка контейнеров..." -ForegroundColor Yellow
docker-compose ps

# Проверка ClickHouse
Write-Host "
🐘 Проверка ClickHouse..." -ForegroundColor Yellow
try {
     = Invoke-WebRequest -Uri "http://localhost:8123/ping" -UseBasicParsing
    if (.Content -eq "Ok") {
        Write-Host "✅ ClickHouse работает" -ForegroundColor Green
        
        # Проверяем базы данных
         = Invoke-WebRequest -Uri "http://localhost:8123/?query=SHOW%20DATABASES" -UseBasicParsing
        Write-Host "   Доступные базы данных:" -ForegroundColor White
        .Content.Split("
") | ForEach-Object { Write-Host "   - " -ForegroundColor White }
    }
} catch {
    Write-Host "❌ ClickHouse не отвечает" -ForegroundColor Red
}

# Проверка AirFlow
Write-Host "
🌀 Проверка AirFlow..." -ForegroundColor Yellow
try {
     = Invoke-WebRequest -Uri "http://localhost:8080/health" -UseBasicParsing
    if (.StatusCode -eq 200) {
        Write-Host "✅ AirFlow работает" -ForegroundColor Green
        
        # Проверяем DAG
        Write-Host "
📊 Проверка DAG..." -ForegroundColor Yellow
        docker exec airflow-webserver airflow dags list
    }
} catch {
    Write-Host "❌ AirFlow не отвечает" -ForegroundColor Red
}

# Проверка логов
Write-Host "
📋 Последние логи AirFlow..." -ForegroundColor Yellow
docker-compose logs --tail=10 airflow-webserver | Select-String -Pattern "ERROR|WARNING|INFO" -CaseSensitive:False

Write-Host "
=== КОМАНДЫ ДЛЯ РАБОТЫ ===" -ForegroundColor Cyan
Write-Host "1. Открыть AirFlow UI: Start-Process 'http://localhost:8080'" -ForegroundColor White
Write-Host "2. Проверить ClickHouse: curl 'http://localhost:8123/?query=SELECT 1'" -ForegroundColor White
Write-Host "3. Логи ClickHouse: docker-compose logs -f clickhouse-server" -ForegroundColor White
Write-Host "4. Логи AirFlow: docker-compose logs -f airflow-webserver" -ForegroundColor White
Write-Host "5. Перезапуск всех: docker-compose restart" -ForegroundColor White
Write-Host "6. Остановка: docker-compose down" -ForegroundColor White

Write-Host "
=== ИНФОРМАЦИЯ ===" -ForegroundColor Green
Write-Host "AirFlow UI: http://localhost:8080 (admin/admin)" -ForegroundColor Yellow
Write-Host "ClickHouse: http://localhost:8123 (admin/clickhouse_pass)" -ForegroundColor Yellow
Write-Host "Flower: http://localhost:5555" -ForegroundColor Yellow

# Открываем AirFlow в браузере
Write-Host "
🌐 Открываю AirFlow в браузере..." -ForegroundColor Cyan
Start-Process "http://localhost:8080"
