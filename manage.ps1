param(
    [string]$Action = "status"
)

function Write-Status {
    param([string]$Message, [string]$Color = "White")
    Write-Host "   $Message" -ForegroundColor $Color
}

switch ($Action.ToLower()) {
    "start" {
        Write-Host "🚀 Запуск всех контейнеров..." -ForegroundColor Cyan
        docker-compose up -d
        Write-Status "✅ Контейнеры запущены" "Green"
        Write-Status "⏳ Ждите 2 минуты для инициализации..." "Yellow"
    }
    
    "stop" {
        Write-Host "🛑 Остановка всех контейнеров..." -ForegroundColor Cyan
        docker-compose down
        Write-Status "✅ Контейнеры остановлены" "Green"
    }
    
    "restart" {
        Write-Host "🔄 Перезапуск всех контейнеров..." -ForegroundColor Cyan
        docker-compose restart
        Write-Status "✅ Контейнеры перезапущены" "Green"
    }
    
    "status" {
        Write-Host "📊 Статус контейнеров:" -ForegroundColor Cyan
        docker-compose ps
    }
    
    "check" {
        Write-Host "🔍 Проверка сервисов:" -ForegroundColor Cyan
        
        # 1. ClickHouse
        Write-Host "
1. ClickHouse:" -ForegroundColor Yellow
        try {
            $response = Invoke-WebRequest -Uri "http://localhost:8123/ping" -TimeoutSec 5 -ErrorAction SilentlyContinue
            if ($response.Content -eq "Ok") {
                Write-Status "✅ Работает (http://localhost:8123)" "Green"
                Write-Status "   Пользователь: admin" "White"
                Write-Status "   Пароль: clickhouse_pass" "White"
            }
        } catch {
            Write-Status "❌ Не отвечает" "Red"
        }
        
        # 2. AirFlow
        Write-Host "
2. AirFlow:" -ForegroundColor Yellow
        try {
            $response = Invoke-WebRequest -Uri "http://localhost:8080" -TimeoutSec 10 -ErrorAction SilentlyContinue
            Write-Status "✅ Работает (http://localhost:8080)" "Green"
            Write-Status "   Логин: admin" "White"
            Write-Status "   Пароль: admin" "White"
        } catch {
            Write-Status "❌ Не отвечает" "Red"
        }
        
        # 3. DBeaver настройки
        Write-Host "
3. Для DBeaver:" -ForegroundColor Yellow
        Write-Status "Хост: localhost" "White"
        Write-Status "Порт: 9000 (Native) или 8123 (HTTP)" "White"
        Write-Status "Пользователь: admin" "White"
        Write-Status "Пароль: clickhouse_pass" "White"
    }
    
    "open" {
        Write-Host "🌐 Открываю веб-интерфейсы..." -ForegroundColor Cyan
        Start-Process "http://localhost:8080"
        Start-Process "http://localhost:8123"
    }
    
    "setup" {
        Write-Host "🛠️ Настройка ClickHouse для hh.ru..." -ForegroundColor Cyan
        
        # 1. Создаем базу
        Write-Host "
1. Создаем базу hh_data..." -ForegroundColor Yellow
        $result = docker exec clickhouse-server clickhouse-client --user admin --password clickhouse_pass --query "CREATE DATABASE IF NOT EXISTS hh_data" 2>&1
        if ($result -eq "") {
            Write-Status "✅ База создана" "Green"
        } else {
            Write-Status "Результат: $result" "White"
        }
        
        # 2. Создаем таблицу
        Write-Host "
2. Создаем таблицу vacancies..." -ForegroundColor Yellow
        $createTable = @'
CREATE TABLE IF NOT EXISTS hh_data.vacancies
(
    id String,
    title String,
    company String,
    city String,
    salary_from Nullable(Float64),
    salary_to Nullable(Float64),
    salary_currency Nullable(String),
    published_date Date,
    published_time DateTime,
    experience String,
    employment String,
    schedule String,
    url String,
    loaded_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(published_date)
ORDER BY (published_date, city, company)
SETTINGS index_granularity = 8192
'@
        
        $result = docker exec clickhouse-server clickhouse-client --user admin --password clickhouse_pass --query ""$createTable"" 2>&1
        if ($result -eq "") {
            Write-Status "✅ Таблица создана" "Green"
        } else {
            Write-Status "Результат: $result" "White"
        }
        
        # 3. Проверяем
        Write-Host "
3. Проверка..." -ForegroundColor Yellow
        $tables = docker exec clickhouse-server clickhouse-client --user admin --password clickhouse_pass --query "SHOW TABLES FROM hh_data" 2>&1
        Write-Status "Таблицы в hh_data:" "White"
        $tables.Split("
") | Where-Object { $_ -ne "" } | ForEach-Object {
            Write-Status "   📊 $_" "White"
        }
    }
    
    "logs" {
        param(
            [string]$Service = ""
        )
        if ($Service) {
            docker-compose logs -f $Service
        } else {
            docker-compose logs -f
        }
    }
    
    default {
        Write-Host "=== УПРАВЛЕНИЕ AIRFLOW + CLICKHOUSE ===" -ForegroundColor Cyan
        Write-Host "Использование: .\manage.ps1 [команда]" -ForegroundColor White
        Write-Host ""
        Write-Host "Команды:" -ForegroundColor Yellow
        Write-Host "  start    - Запустить все контейнеры" -ForegroundColor White
        Write-Host "  stop     - Остановить все контейнеры" -ForegroundColor White
        Write-Host "  restart  - Перезапустить все контейнеры" -ForegroundColor White
        Write-Host "  status   - Показать статус" -ForegroundColor White
        Write-Host "  check    - Проверить работу сервисов" -ForegroundColor White
        Write-Host "  open     - Открыть веб-интерфейсы" -ForegroundColor White
        Write-Host "  setup    - Настроить ClickHouse" -ForegroundColor White
        Write-Host "  logs [service] - Логи контейнера" -ForegroundColor White
        Write-Host ""
        Write-Host "Ссылки:" -ForegroundColor Yellow
        Write-Host "  AirFlow:    http://localhost:8080" -ForegroundColor White
        Write-Host "  ClickHouse: http://localhost:8123" -ForegroundColor White
    }
}
