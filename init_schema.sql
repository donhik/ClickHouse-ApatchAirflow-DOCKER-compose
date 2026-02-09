-- Схема БД для анализа вакансий hh.ru
-- Обновлено: добавлено поле url для ссылок на вакансии

CREATE DATABASE IF NOT EXISTS hh_data;

DROP TABLE IF EXISTS hh_data.vacancies_enhanced;
DROP TABLE IF EXISTS hh_data.vacancies_simple;
DROP TABLE IF EXISTS hh_data.skills;
DROP TABLE IF EXISTS hh_data.skills_agg;

-- Расширенная таблица с детальной информацией
CREATE TABLE hh_data.vacancies_enhanced (
    id String,
    name String,
    employer String,
    salary_from Nullable(Float64),
    salary_to Nullable(Float64),
    salary_currency String,
    experience String,
    employment String,
    schedule String,
    city String,
    url String,          -- Ссылка на вакансию на hh.ru
    published_at DateTime,
    created_at DateTime
) ENGINE = MergeTree()
ORDER BY (city, created_at);

-- Упрощённая таблица для веб-интерфейса
CREATE TABLE hh_data.vacancies_simple (
    id String,
    name String,
    employer String,
    salary_from Nullable(Float64),
    salary_to Nullable(Float64),
    salary_currency String,
    city String,
    url String,          -- Ссылка на вакансию на hh.ru
    published_at DateTime,
    created_at DateTime
) ENGINE = MergeTree()
ORDER BY (city, created_at);

-- Таблица навыков (для будущей аналитики)
CREATE TABLE hh_data.skills (
    vacancy_id String,
    skill_name String,
    city String,
    created_at DateTime
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY (city, skill_name, vacancy_id);

-- Агрегированная таблица навыков
CREATE TABLE hh_data.skills_agg (
    skill_name String,
    city String,
    vacancy_count UInt64,
    avg_salary_from Float64,
    updated_at DateTime
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (city, skill_name);
