Write-Host "=== Развертывание AirFlow + ClickHouse ===" -ForegroundColor Cyan

# Проверка Docker
Write-Host "Проверка Docker..." -ForegroundColor Yellow
try {
    $dockerVersion = docker --version
    Write-Host "Docker установлен: $dockerVersion" -ForegroundColor Green
} catch {
    Write-Host "Ошибка: Docker не установлен или не запущен!" -ForegroundColor Red
    exit 1
}

# Проверка Docker Compose
Write-Host "Проверка Docker Compose..." -ForegroundColor Yellow
try {
    $composeVersion = docker-compose --version
    Write-Host "Docker Compose установлен: $composeVersion" -ForegroundColor Green
} catch {
    Write-Host "Предупреждение: docker-compose не найден, пробуем docker compose..." -ForegroundColor Yellow
    try {
        $composeVersion = docker compose version
        Write-Host "Docker Compose установлен: $composeVersion" -ForegroundColor Green
    } catch {
        Write-Host "Ошибка: Docker Compose не установлен!" -ForegroundColor Red
        exit 1
    }
}

# Создаем необходимые папки
Write-Host "Создание структуры папок..." -ForegroundColor Yellow
New-Item -ItemType Directory -Force -Path "logs", "plugins", "dags" | Out-Null
New-Item -ItemType Directory -Force -Path "config\clickhouse" | Out-Null

# Запуск контейнеров
Write-Host "Запуск контейнеров..." -ForegroundColor Yellow
docker-compose up -d

Write-Host "Ожидание запуска сервисов (60 секунд)..." -ForegroundColor Yellow
Start-Sleep -Seconds 60

# Проверка статуса
Write-Host "
Проверка статуса контейнеров..." -ForegroundColor Cyan
docker-compose ps

Write-Host "
=== СЕРВИСЫ ДОСТУПНЫ ===" -ForegroundColor Green
Write-Host "AirFlow Web UI: http://localhost:8080" -ForegroundColor Yellow
Write-Host "  Логин: admin" -ForegroundColor White
Write-Host "  Пароль: admin" -ForegroundColor White
Write-Host ""
Write-Host "ClickHouse HTTP интерфейс: http://localhost:8123" -ForegroundColor Yellow
Write-Host "ClickHouse Native порт: localhost:9000" -ForegroundColor Yellow
Write-Host ""
Write-Host "Flower (мониторинг Celery): http://localhost:5555" -ForegroundColor Yellow
Write-Host ""
Write-Host "=== КОМАНДЫ ДЛЯ УПРАВЛЕНИЯ ===" -ForegroundColor Cyan
Write-Host "Остановка: docker-compose down" -ForegroundColor White
Write-Host "Просмотр логов: docker-compose logs -f [service]" -ForegroundColor White
Write-Host "Перезапуск: docker-compose restart [service]" -ForegroundColor White
Write-Host ""
Write-Host "=== ТЕСТИРОВАНИЕ ===" -ForegroundColor Cyan
Write-Host "Проверка ClickHouse: curl http://localhost:8123/ping" -ForegroundColor White
Write-Host ""
Write-Host "Развертывание завершено!" -ForegroundColor Green
