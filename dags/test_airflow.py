from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
}

def test_task():
    print("=" * 50)
    print("🎉 AirFlow работает!")
    print(f"Время: {datetime.now()}")
    print("=" * 50)
    return "Success"

with DAG(
    'test_airflow',
    default_args=default_args,
    description='Тестовый DAG для проверки AirFlow',
    schedule_interval='@once',
    catchup=False,
    tags=['test']
) as dag:
    
    start = DummyOperator(task_id='start')
    
    test = PythonOperator(
        task_id='test_task',
        python_callable=test_task
    )
    
    end = DummyOperator(task_id='end')
    
    start >> test >> end
