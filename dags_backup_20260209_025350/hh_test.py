"""
ПРОСТОЙ РАБОЧИЙ DAG ДЛЯ HH.RU
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import json

def test_hh_api():
    """Тестовый DAG для проверки"""
    print("=" * 60)
    print("🧪 ТЕСТОВЫЙ DAG ДЛЯ HH.RU")
    print("=" * 60)
    
    # 1. Тест hh.ru API
    print("\n1. Тестируем API hh.ru...")
    try:
        url = "https://api.hh.ru/vacancies"
        params = {"text": "python", "per_page": 2}
        response = requests.get(url, params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            print(f"   ✅ API работает. Найдено вакансий: {data.get('found', 0)}")
            
            # Показываем примеры
            for i, item in enumerate(data.get('items', [])[:2], 1):
                print(f"   {i}. {item.get('name', '')[:40]}...")
        else:
            print(f"   ❌ Ошибка API: {response.status_code}")
    except Exception as e:
        print(f"   ❌ Ошибка: {e}")
    
    # 2. Тест ClickHouse
    print("\n2. Тестируем ClickHouse...")
    try:
        import urllib.parse
        
        # Простой запрос
        query = "SELECT 1 as test, now() as time"
        encoded = urllib.parse.quote(query)
        url = f"http://clickhouse-server:8123/?query={encoded}&user=admin&password=clickhouse_pass"
        
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            print(f"   ✅ ClickHouse работает: {response.text}")
        else:
            print(f"   ❌ Ошибка: {response.status_code}")
    except Exception as e:
        print(f"   ❌ Ошибка ClickHouse: {e}")
    
    print("\n" + "=" * 60)
    print("✅ ТЕСТ ЗАВЕРШЕН")
    print("=" * 60)
    
    return "Готово"

def get_real_vacancies():
    """Получает реальные вакансии"""
    print("🎯 Получение вакансий с hh.ru")
    
    try:
        # Поисковые запросы
        queries = ["Python разработчик", "Data Engineer", "Аналитик данных"]
        
        all_vacancies = []
        
        for query in queries:
            print(f"\n🔍 Поиск: '{query}'")
            
            url = "https://api.hh.ru/vacancies"
            params = {
                "text": query,
                "area": 1,  # Москва
                "per_page": 5,
                "page": 0
            }
            headers = {"User-Agent": "DataPipeline/1.0"}
            
            response = requests.get(url, params=params, headers=headers, timeout=20)
            
            if response.status_code == 200:
                data = response.json()
                vacancies = data.get("items", [])
                
                print(f"   Найдено: {len(vacancies)} вакансий")
                
                # Обрабатываем
                for v in vacancies:
                    vacancy = {
                        "id": v.get("id", ""),
                        "title": v.get("name", ""),
                        "company": v.get("employer", {}).get("name", ""),
                        "city": v.get("area", {}).get("name", ""),
                        "salary": v.get("salary", {}),
                        "published": v.get("published_at", ""),
                        "url": v.get("alternate_url", "")
                    }
                    all_vacancies.append(vacancy)
        
        print(f"\n📊 Всего собрано вакансий: {len(all_vacancies)}")
        
        # Сохраняем в файл
        import os
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"/tmp/hh_vacancies_{timestamp}.json"
        
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(all_vacancies, f, ensure_ascii=False, indent=2)
        
        print(f"💾 Данные сохранены: {filename}")
        
        # Показываем пример
        if all_vacancies:
            print("\n📋 Примеры вакансий:")
            for i, v in enumerate(all_vacancies[:3], 1):
                print(f"{i}. {v['title'][:40]}... | {v['company']} | {v['city']}")
        
        return f"Собрано {len(all_vacancies)} вакансий"
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return f"Ошибка: {e}"

# Создаем DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'hh_test_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['hh.ru', 'test']
) as dag:
    
    test_task = PythonOperator(
        task_id='test_api',
        python_callable=test_hh_api
    )
    
    vacancies_task = PythonOperator(
        task_id='get_vacancies',
        python_callable=get_real_vacancies
    )
    
    test_task >> vacancies_task
