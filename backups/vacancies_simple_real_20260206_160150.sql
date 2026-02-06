-- Реальная таблица vacancies_simple с первого ПК
-- Создана автоматически, содержит данные HH
-- Создан: 02/06/2026 16:01:50

CREATE DATABASE IF NOT EXISTS hh_data;

USE hh_data;

CREATE TABLE IF NOT EXISTS hh_data.vacancies_simple\n(\n    `id` String,\n    `title` String,\n    `company` String,\n    `city` String,\n    `salary` Float64,\n    `published_date` Date,\n    `url` String,\n    `loaded_at` DateTime DEFAULT now(),\n    `salary_from` Nullable(Float64),\n    `salary_to` Nullable(Float64),\n    `salary_currency` Nullable(String)\n)\nENGINE = MergeTree\nORDER BY (published_date, city)\nSETTINGS index_granularity = 8192
