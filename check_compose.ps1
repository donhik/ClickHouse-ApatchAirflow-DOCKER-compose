# Проверка Docker Compose файла
Write-Host "Проверка docker-compose.yml..." -ForegroundColor Cyan

# 1. Проверка синтаксиса
try {
    docker-compose config > 
    Write-Host "✓ Синтаксис docker-compose.yml корректен" -ForegroundColor Green
} catch {
    Write-Host "✗ Ошибка в docker-compose.yml: " -ForegroundColor Red
    exit 1
}

# 2. Проверка существования конфигов
if (Test-Path .\config\clickhouse\users.xml) {
    Write-Host "✓ Конфигурационный файл users.xml существует" -ForegroundColor Green
} else {
    Write-Host "✗ Файл users.xml не найден" -ForegroundColor Yellow
}

# 3. Проверка структуры базы данных
if (Test-Path .\database_dumps\clickhouse\schema.sql) {
    Write-Host "✓ SQL схема базы данных существует" -ForegroundColor Green
} else {
    Write-Host "✗ SQL схема не найдена" -ForegroundColor Yellow
}

Write-Host "
Для запуска выполните: docker-compose up -d" -ForegroundColor Cyan
