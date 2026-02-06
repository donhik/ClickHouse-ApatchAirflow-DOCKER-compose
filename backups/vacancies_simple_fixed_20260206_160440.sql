-- Правильный дамп таблицы vacancies_simple
-- Создан: 02/06/2026 16:04:40
-- Без обратных кавычек для совместимости с ClickHouse

CREATE DATABASE IF NOT EXISTS hh_data;

USE hh_data;

CREATE TABLE IF NOT EXISTS hh_data.vacancies_simple\n(\n    id String,\n    title String,\n    company String,\n    city String,\n    salary Float64,\n    published_date Date,\n    url String,\n    loaded_at DateTime DEFAULT now(),\n    salary_from Nullable(Float64),\n    salary_to Nullable(Float64),\n    salary_currency Nullable(String)\n)\nENGINE = MergeTree\nORDER BY (published_date, city)\nSETTINGS index_granularity = 8192
