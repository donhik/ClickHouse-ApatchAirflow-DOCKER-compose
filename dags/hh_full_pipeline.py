"""
ПОЛНЫЙ ПАЙПЛАЙН ДЛЯ HH.RU
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import urllib.parse
import json

def hh_full_pipeline():
    """Полный пайплайн сбора и сохранения данных"""
    print("=" * 60)
    print("🚀 ПОЛНЫЙ ПАЙПЛАЙН HH.RU -> CLICKHOUSE")
    print("=" * 60)
    
    try:
        # 1. Получаем данные с hh.ru
        print("\n1. Получение данных с hh.ru...")
        
        search_queries = [
            {"text": "Python разработчик", "area": 1},
            {"text": "Data Engineer", "area": 1},
            {"text": "Аналитик данных", "area": 1}
        ]
        
        all_vacancies = []
        
        for query in search_queries:
            print(f"   🔍 Поиск: {query['text']}")
            
            url = "https://api.hh.ru/vacancies"
            params = {
                "text": query["text"],
                "area": query["area"],
                "per_page": 10,
                "page": 0,
                "period": 1  # За последний день
            }
            headers = {"User-Agent": "HH-Pipeline/1.0"}
            
            response = requests.get(url, params=params, headers=headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                vacancies = data.get("items", [])
                
                for v in vacancies:
                    # Обрабатываем зарплату
                    salary_from = None
                    salary_to = None
                    salary_currency = None
                    
                    salary = v.get("salary")
                    if salary:
                        if salary.get("from"):
                            salary_from = float(salary["from"])
                        if salary.get("to"):
                            salary_to = float(salary["to"])
                        salary_currency = salary.get("currency")
                    
                    # Формируем запись
                    vacancy = {
                        "id": v.get("id", ""),
                        "title": v.get("name", ""),
                        "company": v.get("employer", {}).get("name", ""),
                        "city": v.get("area", {}).get("name", "Не указано"),
                        "salary_from": salary_from,
                        "salary_to": salary_to,
                        "salary_currency": salary_currency,
                        "published_date": v.get("published_at", "")[:10],
                        "url": v.get("alternate_url", "")
                    }
                    
                    all_vacancies.append(vacancy)
                
                print(f"      Найдено: {len(vacancies)} вакансий")
        
        print(f"\n📊 Всего собрано: {len(all_vacancies)} вакансий")
        
        if not all_vacancies:
            print("❌ Нет данных для обработки")
            return
        
        # 2. Сохраняем в ClickHouse
        print("\n2. Сохранение в ClickHouse...")
        
        # Подготавливаем данные для вставки
        values = []
        for v in all_vacancies:
            # Экранируем кавычки
            title = v["title"].replace("'", "''")
            company = v["company"].replace("'", "''")
            city = v["city"].replace("'", "''")
            
            # Формируем VALUES
            value = f"""(
                '{v["id"]}',
                '{title}',
                '{company}',
                '{city}',
                {v["salary_from"] or 'NULL'},
                {v["salary_to"] or 'NULL'},
                {f"'{v['salary_currency']}'" if v["salary_currency"] else 'NULL'},
                '{v["published_date"]}',
                '{v["url"]}'
            )"""
            
            values.append(value)
        
        # SQL для вставки
        insert_sql = f"""
        INSERT INTO hh_data.vacancies_simple 
        (id, title, company, city, salary_from, salary_to, salary_currency, published_date, url)
        VALUES {','.join(values)}
        """
        
        # Выполняем запрос
        encoded_sql = urllib.parse.quote(insert_sql)
        url = f"http://clickhouse-server:8123/?query={encoded_sql}&user=admin&password=clickhouse_pass"
        
        response = requests.post('http://clickhouse-server:8123/', params={'query': insert_sql, 'user': 'admin', 'password': 'clickhouse_pass'}, timeout=30)
        
        if response.status_code == 200:
            print("✅ Данные успешно сохранены в ClickHouse")
        else:
            print(f"❌ Ошибка сохранения: {response.status_code} - {response.text}")
        
        # 3. Генерируем отчет
        print("\n3. Генерация отчета...")
        
        # Получаем статистику
        stats_url = "http://clickhouse-server:8123/?query="
        
        stats_queries = {
            "Всего вакансий": "SELECT count() FROM hh_data.vacancies_simple",
            "По городам": """
                SELECT city, count() as count 
                FROM hh_data.vacancies_simple 
                GROUP BY city 
                ORDER BY count DESC 
                LIMIT 5
            """,
            "Топ компаний": """
                SELECT company, count() as vacancies 
                FROM hh_data.vacancies_simple 
                GROUP BY company 
                ORDER BY vacancies DESC 
                LIMIT 5
            """
        }
        
        report = ["=" * 60]
        report.append("📈 ОТЧЕТ ПО ВАКАНСИЯМ")
        report.append(f"Дата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("=" * 60)
        
        for title, query in stats_queries.items():
            encoded = urllib.parse.quote(query)
            url = f"http://clickhouse-server:8123/?query={encoded}&user=admin&password=clickhouse_pass"
            
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                report.append(f"\n{title}:")
                lines = response.text.strip().split('\n')
                for line in lines:
                    if line:
                        report.append(f"  {line}")
        
        report.append("\n" + "=" * 60)
        
        # Выводим отчет
        for line in report:
            print(line)
        
        # Сохраняем отчет
        with open(f"/tmp/hh_report.txt", "w", encoding="utf-8") as f:
            f.write("\n".join(report))
        
        print("💾 Отчет сохранен в /tmp/hh_report.txt")
        
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
    
    print("\n" + "=" * 60)
    print("✅ ПАЙПЛАЙН ЗАВЕРШЕН")
    print("=" * 60)
    
    return f"Обработано {len(all_vacancies)} вакансий"

# Создаем DAG
default_args = {
    'owner': 'data_team',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'hh_full_pipeline',
    default_args=default_args,
    description='Полный пайплайн сбора вакансий',
    schedule_interval='0 8 * * *',  # Каждый день в 8:00
    catchup=False,
    tags=['hh.ru', 'production', 'clickhouse']
) as dag:
    
    main_task = PythonOperator(
        task_id='run_full_pipeline',
        python_callable=hh_full_pipeline
    )
