import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def test_clickhouse():
    sql = "INSERT INTO hh_data.vacancies_simple (id, title, company, city) VALUES (\"test_$(Get-Random)\", \"Test\", \"Company\", \"City\")"
    response = requests.post("http://clickhouse-server:8123/", params={"query": sql, "user": "admin", "password": "clickhouse_pass"})
    return response.text

with DAG("quick_test", start_date=datetime(2024,1,1), schedule_interval=None) as dag:
    task = PythonOperator(task_id="test", python_callable=test_clickhouse)
