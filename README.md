
# 🚀 VacancyFlow: Анализ вакансий с hh.ru на Airflow + ClickHouse

> **VacancyFlow** — готовое решение для автоматического мониторинга вакансий с [hh.ru](https://hh.ru) в трёх городах России. Собирает данные каждые 4 часа, отображает аналитику в веб-интерфейсе и позволяет **переходить к вакансии одним кликом**.

  🗂️ Структура проекта
ClickHouse-ApatchAirflow-DOCKER-compose/
├── docker-compose.yml          # Оркестрация: Airflow + ClickHouse + веб-интерфейс
├── init_schema.sql             # Схема БД с полем `url` для ссылок на вакансии
├── dags/
│   └── hh_3cities_stable.py    # ЕДИНСТВЕННЫЙ рабочий DAG (Москва, Краснодар, Волгоград)
├── README.md                   # Эта документация
└── .gitignore

## ⚙️ Быстрый старт (Windows + PowerShell)

```powershell
# 1. Клонировать репозиторий
git clone https://github.com/donhik/ClickHouse-ApatchAirflow-DOCKER-compose.git
cd ClickHouse-ApatchAirflow-DOCKER-compose

# 2. Запустить стек (первый запуск ~2 минуты)
docker-compose up -d

# 3. Применить схему БД
docker-compose exec -T clickhouse-server clickhouse-client --user admin --password clickhouse_pass --multiquery < init_schema.sql

# 4. Запустить первый сбор данных
docker-compose exec -T airflow-webserver airflow dags unpause hh_3cities_stable
docker-compose exec -T airflow-webserver airflow dags trigger hh_3cities_stable

# 5. Открыть дашборд
Start-Process "http://localhost:5001"
⏱️ Через 2-3 минуты данные появятся в дашборде!

## 🖥️ Интерфейсы после запуска

| Интерфейс | URL | Логин/Пароль |
|-----------|-----|--------------|
| **📊 Веб-дашборд** | `http://localhost:5001` | — |
| **🤖 Airflow UI** | `http://localhost:8080` | `airflow` / `airflow` |
| **🗄️ ClickHouse** | `http://localhost:8123` | `admin` / `clickhouse_pass` |

> 💡 **В дашборде:** Топ вакансий, средняя зарплата по городам, кнопка **«Открыть»** для перехода к полному описанию на hh.ru

📈 Как работает сбор данных
$readmeContent = Get-Content -Path "README.md" -Raw
$readmeContent = $readmeContent -replace "```mermaid.*?```", "![Схема сбора данных](docs/diagram.png)"
Set-Content -Path "README.md" -Value $readmeContent -Encoding UTF8
