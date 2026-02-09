from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import logging

logger = logging.getLogger(__name__)

def safe_get(obj, *keys, default=None):
    """"""Безопасное извлечение вложенных значений из словаря""""""
    for key in keys:
        if isinstance(obj, dict):
            obj = obj.get(key)
        else:
            return default
        if obj is None:
            return default
    return obj if obj is not None else default

def fetch_all_cities(**context):
    """"""Сбор вакансий из 3 городов: Москва, Краснодар, Волгоград""""""
    cities = [
        {"id": 1, "name": "Москва"},
        {"id": 16, "name": "Краснодар"},
        {"id": 15, "name": "Волгоград"}
    ]
    all_rows_enhanced = []
    all_rows_simple = []
    
    for city in cities:
        logger.info(f""📍 Запрос вакансий для {city['name']} (area={city['id']})..."")
        
        try:
            # Единственный запрос к поисковому API (без детализации → без 403 ошибок)
            resp = requests.get(
                ""https://api.hh.ru/vacancies"",
                params={""area"": city[""id""], ""text"": ""python"", ""per_page"": 40},
                headers={""User-Agent"": ""Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36""},
                timeout=25
            )
            
            logger.info(f""📡 {city['name']}: HTTP {resp.status_code}"")
            
            # Пропускаем неудачные запросы без падения всего пайплайна
            if resp.status_code != 200:
                logger.warning(f""⚠️ {city['name']}: статус {resp.status_code}, пропускаем"")
                continue
            
            data = resp.json()
            items = safe_get(data, ""items"", default=[])
            
            if not items:
                logger.warning(f""⚠️ {city['name']}: нет вакансий в ответе"")
                continue
            
            logger.info(f""✅ {city['name']}: получено {len(items)} вакансий"")
            
            # Обрабатываем до 30 вакансий на город
            for item in items[:30]:
                item_id = str(safe_get(item, ""id"", default=""unknown""))
                name = str(safe_get(item, ""name"", default=""Вакансия"")).replace(""'"", ""''"")[:200]
                
                employer = safe_get(item, ""employer"", ""name"", default=""Не указано"")
                employer = str(employer).replace(""'"", ""''"")[:200] if employer else ""Не указано""
                
                salary = safe_get(item, ""salary"")
                salary_from = safe_get(salary, ""from"")
                salary_to = safe_get(salary, ""to"")
                currency = safe_get(salary, ""currency"", default=""RUR"") or ""RUR""
                
                experience = safe_get(item, ""experience"", ""name"", default=""Не указано"")
                employment = safe_get(item, ""employment"", ""name"", default=""Полная занятость"")
                schedule = safe_get(item, ""schedule"", ""name"", default=""Полный день"")
                
                # Извлекаем человекочитаемую ссылку на вакансию (поле alternate_url)
                url = safe_get(item, ""alternate_url"", default=f""https://hh.ru/vacancy/{item_id}"")
                url = str(url).replace(""'"", ""''"")[:500]
                
                published_at = safe_get(item, ""published_at"", default=datetime.utcnow().isoformat())
                
                # Формируем строки для вставки в обе таблицы
                row_enhanced = f""('{item_id}', '{name}', '{employer}', {salary_from or 'NULL'}, {salary_to or 'NULL'}, '{currency}', '{experience}', '{employment}', '{schedule}', '{city['name']}', '{url}', parseDateTimeBestEffortOrNull('{published_at}'), now())""
                row_simple = f""('{item_id}', '{name}', '{employer}', {salary_from or 'NULL'}, {salary_to or 'NULL'}, '{currency}', '{city['name']}', '{url}', parseDateTimeBestEffortOrNull('{published_at}'), now())""
                
                all_rows_enhanced.append(row_enhanced)
                all_rows_simple.append(row_simple)
                
        except Exception as e:
            logger.warning(f""⚠️ {city['name']}: ошибка {str(e)[:120]}"")
            continue
    
    # Если нет данных — логируем ошибку и выходим
    if not all_rows_enhanced:
        logger.error(""❌ НЕТ ДАННЫХ ДЛЯ ЗАГРУЗКИ. Возможные причины:"")
        logger.error(""   • API hh.ru временно недоступно"")
        logger.error(""   • Блокировка по IP (слишком много запросов)"")
        logger.error(""   • Проблема с сетью в контейнере"")
        return
    
    # Загрузка в таблицу vacancies_enhanced
    try:
        insert_sql = ""INSERT INTO hh_data.vacancies_enhanced (id, name, employer, salary_from, salary_to, salary_currency, experience, employment, schedule, city, url, published_at, created_at) VALUES "" + "", "".join(all_rows_enhanced)
        requests.post(
            ""http://clickhouse-server:8123"",
            params={
                ""user"": ""admin"",
                ""password"": ""clickhouse_pass"",
                ""database"": ""hh_data"",
                ""query"": insert_sql
            },
            timeout=30
        )
        logger.info(f""✅ Загружено {len(all_rows_enhanced)} вакансий в vacancies_enhanced"")
    except Exception as e:
        logger.error(f""❌ Ошибка загрузки в vacancies_enhanced: {str(e)[:150]}"")
    
    # Загрузка в таблицу vacancies_simple (для веб-интерфейса)
    try:
        insert_sql = ""INSERT INTO hh_data.vacancies_simple (id, name, employer, salary_from, salary_to, salary_currency, city, url, published_at, created_at) VALUES "" + "", "".join(all_rows_simple)
        requests.post(
            ""http://clickhouse-server:8123"",
            params={
                ""user"": ""admin"",
                ""password"": ""clickhouse_pass"",
                ""database"": ""hh_data"",
                ""query"": insert_sql
            },
            timeout=30
        )
        logger.info(f""✅ Загружено {len(all_rows_simple)} вакансий в vacancies_simple"")
    except Exception as e:
        logger.error(f""❌ Ошибка загрузки в vacancies_simple: {str(e)[:150]}"")
    
    logger.info(f""🎉 Успешно собрано {len(all_rows_enhanced)} вакансий из 3 городов"")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    'hh_3cities_stable',
    default_args=default_args,
    description='✅ Стабильный сбор вакансий из Москвы, Краснодара, Волгограда + ссылки на объявления',
    schedule_interval='0 */4 * * *',  # Каждые 4 часа
    catchup=False,
    tags=['hh.ru', 'moscow', 'krasnodar', 'volgograd', 'stable'],
) as dag:

    PythonOperator(
        task_id='fetch_all_cities',
        python_callable=fetch_all_cities,
        provide_context=True,
    )
