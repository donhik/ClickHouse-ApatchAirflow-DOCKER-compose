Автоматизированный сбор и анализ вакансий с [hh.ru](https://hh.ru) для трёх городов России: **Москва, Краснодар, Волгоград**.

## 🚀 Особенности

- ✅ **Стабильный сбор данных** каждые 4 часа без ошибок 403
- ✅ **Ссылки на вакансии** — возможность мгновенного перехода к объявлению на hh.ru
- ✅ **Минималистичная архитектура** — один DAG вместо множества ненужных пайплайнов
- ✅ **Веб-интерфейс** с аналитикой по зарплатам и распределению вакансий
- ✅ **Параллельная обработка** трёх городов для скорости

## 📊 Демо

Веб-интерфейс после запуска: `http://localhost:5001`

![Дашборд](docs/dashboard-preview.png)

## 🗂️ Структура проекта
├── docker-compose.yml # Определение сервисов (Airflow, ClickHouse, веб-интерфейс)
├── init_schema.sql # Схема базы данных с полем url для ссылок
├── dags/
│ └── hh_3cities_stable.py # ЕДИНСТВЕННЫЙ стабильный DAG для сбора данных
├── scripts/
│ ├── apply_schema.ps1 # Применение схемы к ClickHouse (PowerShell)
│ └── apply_schema.sh # Применение схемы к ClickHouse (Bash)
└── README.md # Эта документация

## ⚙️ Быстрый старт (Windows + PowerShell)

```powershell
# 1. Клонировать репозиторий
git clone https://github.com/donhik/ClickHouse-ApatchAirflow-DOCKER-compose.git
cd ClickHouse-ApatchAirflow-DOCKER-compose

# 2. Запустить стек
docker-compose up -d

# 3. Применить схему БД
.\scripts\apply_schema.ps1

# 4. Открыть интерфейсы:
#    • Веб-интерфейс:   http://localhost:5001
#    • Airflow UI:      http://localhost:8080 (логин: airflow, пароль: airflow)
🌐 Доступ к интерфейсам
Сервис	Порт	Описание
Веб-интерфейс	http://localhost:5001	Дашборд с аналитикой и ссылками на вакансии
Airflow UI	http://localhost:8080	Управление пайплайнами (логин: admin / пароль: admin)
ClickHouse HTTP	http://localhost:8123	Прямой доступ к данным
