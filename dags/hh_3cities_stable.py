from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import logging
logger = logging.getLogger(__name__)
def safe_get(obj, *keys, default=None):
    for key in keys:
        if isinstance(obj, dict): obj = obj.get(key)
        else: return default
        if obj is None: return default
    return obj if obj is not None else default
def fetch_all_cities(**context):
    cities = [{"id":1,"name":"Moscow"},{"id":16,"name":"Krasnodar"},{"id":15,"name":"Volgograd"}]
    all_rows = []
    for city in cities:
        try:
            resp = requests.get("https://api.hh.ru/vacancies", params={"area":city["id"],"text":"python","per_page":40}, headers={"User-Agent":"Mozilla/5.0"}, timeout=25)
            if resp.status_code != 200: continue
            items = resp.json().get("items", [])
            if not items: continue
            for item in items[:30]:
                item_id = str(item.get("id", "unknown"))
                name = str(item.get("name", "Vacancy")).replace("'", "''")[:200]
                employer = str(item.get("employer", {}).get("name", "Unknown")).replace("'", "''")[:200] if isinstance(item.get("employer"), dict) else "Unknown"
                salary = item.get("salary")
                salary_from = salary.get("from") if isinstance(salary, dict) else None
                salary_to = salary.get("to") if isinstance(salary, dict) else None
                currency = salary.get("currency", "RUR") if isinstance(salary, dict) else "RUR"
                experience = item.get("experience", {}).get("name", "Not specified")
                employment = item.get("employment", {}).get("name", "Full employment")
                schedule = item.get("schedule", {}).get("name", "Full day")
                url = item.get("alternate_url", "https://hh.ru/vacancy/" + item_id)
                url = str(url).replace("'", "''")[:500]
                published_at = item.get("published_at", datetime.utcnow().isoformat())
                row = "('" + item_id + "', '" + name + "', '" + employer + "', " + (str(salary_from) if salary_from else "NULL") + ", " + (str(salary_to) if salary_to else "NULL") + ", '" + currency + "', '" + experience + "', '" + employment + "', '" + schedule + "', '" + city["name"] + "', '" + url + "', parseDateTimeBestEffortOrNull('" + published_at + "'), now())"
                all_rows.append(row)
        except Exception as e: logger.warning("Error: " + str(e)[:100]); continue
    if not all_rows: logger.error("No data to load"); return
    try:
        sql = "INSERT INTO hh_data.vacancies_enhanced (id, name, employer, salary_from, salary_to, salary_currency, experience, employment, schedule, city, url, published_at, created_at) VALUES " + ", ".join(all_rows)
        requests.post("http://clickhouse-server:8123", params={"user":"admin","password":"clickhouse_pass","database":"hh_data","query":sql}, timeout=30)
        logger.info("Loaded " + str(len(all_rows)) + " vacancies")
    except Exception as e: logger.error("DB error: " + str(e)[:100])
default_args = {'owner':'airflow','depends_on_past':False,'email_on_failure':False,'retries':1,'retry_delay':timedelta(minutes=5),'start_date':datetime(2024,1,1)}
with DAG('hh_3cities_stable', default_args=default_args, description='Stable collection to ONE table', schedule_interval='0 */4 * * *', catchup=False, tags=['hh.ru','stable']) as dag:
    PythonOperator(task_id='fetch_all_cities', python_callable=fetch_all_cities, provide_context=True)
