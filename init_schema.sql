CREATE DATABASE IF NOT EXISTS hh_data;
DROP TABLE IF EXISTS hh_data.vacancies_enhanced;
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
    url String,
    published_at DateTime,
    created_at DateTime
) ENGINE = MergeTree()
ORDER BY (city, created_at);
