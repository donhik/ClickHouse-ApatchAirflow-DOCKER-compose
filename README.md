# 🚀 ClickHouse + Apache Airflow Docker Compose

## 📊 Проект для анализа вакансий HH.ru

Автоматизированная система сбора, обработки и анализа вакансий с платформы HH.ru с использованием Apache Airflow для оркестрации задач и ClickHouse для хранения и аналитики данных.

## ✨ Возможности

- 📥 Автоматический сбор вакансий с HH.ru API по расписанию
- 💾 Хранение данных в высокопроизводительной колоночной СУБД ClickHouse
- ⚡ Быстрая аналитика благодаря оптимизированной структуре ClickHouse
- 📈 Готовые отчеты по вакансиям, зарплатам и компаниям
- 🐳 Простое развертывание через Docker Compose

## 📅 Выполняемые DAG (пайплайны)

### 🔄 hh_full_pipeline
- **Расписание:** Ежедневно в 8:00
- **Описание:** Полный цикл сбора вакансий
- **Что делает:**
  1. Получает вакансии с HH.ru API
  2. Обрабатывает данные (очистка, преобразование)
  3. Загружает в ClickHouse
  4. Генерирует отчет

### 🔧 hh_ru_pipeline
- **Расписание:** Ежедневно в 2:00
- **Описание:** Расширенный пайплайн с дополнительной обработкой
- **Что делает:**
  1. Сбор расширенных данных о вакансиях
  2. Обработка навыков (skills) и требований
  3. Анализ метро и местоположений

## 🚀 Быстрый старт

### 1. Клонирование репозитория
~~~
git clone https://github.com/donhik/ClickHouse-ApatchAirflow-DOCKER-compose.git
cd ClickHouse-ApatchAirflow-DOCKER-compose
~~~

### 2. Запуск проекта
~~~
# Запуск в фоновом режиме
docker-compose up -d

# Просмотр логов
docker-compose logs -f
~~~

## 🌐 Доступ к интерфейсам

| Сервис | URL | Учетные данные |
|--------|-----|----------------|
| **Airflow UI** | http://localhost:8080 | airflow / airflow |
| **ClickHouse HTTP** | http://localhost:8123 | admin / clickhouse_pass |

## 🛠️ Полезные команды

### Управление DAG
~~~
# Запустить основной DAG вручную
docker-compose exec airflow-webserver airflow dags trigger hh_full_pipeline

# Проверить статус выполнения
docker-compose exec airflow-webserver airflow dags list-runs -d hh_full_pipeline
~~~

### Работа с данными
~~~
# Подключиться к ClickHouse
docker-compose exec clickhouse-server clickhouse-client --user admin --password clickhouse_pass

# Пример запроса: количество вакансий
SELECT COUNT(*) FROM hh_data.vacancies_simple;
~~~

## 📊 Структура данных

В ClickHouse создается таблица hh_data.vacancies_simple с колонками:
- id, title, company, city
- salary_from, salary_to, salary_currency
- published_date, url, loaded_at

---
⭐ **Если проект полезен, поставьте звезду на GitHub!**


## 🌐 Доступ к сервисам

| Сервис | URL | Логин/Пароль |
|--------|-----|--------------|
| **Веб-интерфейс** | **http://localhost:5001** | — |
| **Airflow UI** | http://localhost:8080 | admin/admin |
| **ClickHouse HTTP** | http://localhost:8123 | admin/clickhouse_pass |

