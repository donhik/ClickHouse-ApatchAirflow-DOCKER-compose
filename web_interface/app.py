from flask import Flask, render_template
import os
from clickhouse_driver import Client
from datetime import datetime

app = Flask(__name__)

def get_clickhouse_client():
    try:
        return Client(
            host=os.getenv('CLICKHOUSE_HOST', 'clickhouse-server'),
            port=int(os.getenv('CLICKHOUSE_PORT', 9000)),
            user=os.getenv('CLICKHOUSE_USER', 'admin'),
            password=os.getenv('CLICKHOUSE_PASSWORD', 'clickhouse_pass'),
            database=os.getenv('CLICKHOUSE_DATABASE', 'hh_data')
        )
    except Exception as e:
        print(f'ClickHouse connection error: {e}')
        return None

@app.route('/')
def index():
    client = get_clickhouse_client()
    
    stats = {
        "total_vacancies": 0,
        "avg_salary": 0,
        "top_companies": [],
        "recent_vacancies": [],
        "last_update": "N/A"
    }
    
    airflow_status = {"status": "error"}
    
    if client:
        try:
            # 1. Общее количество вакансий
            result = client.execute('SELECT COUNT(*) FROM hh_data.vacancies_enhanced')
            stats["total_vacancies"] = result[0][0] if result and result[0] else 0
            
            # 2. Средняя зарплата (только в рублях, с проверкой на NULL)
            result = client.execute('''
                SELECT ROUND(AVG((salary_from + salary_to) / 2)) 
                FROM hh_data.vacancies_enhanced 
                WHERE salary_from IS NOT NULL 
                  AND salary_to IS NOT NULL 
                  AND salary_currency IN ('RUR', 'RUB')
            ''')
            stats["avg_salary"] = int(result[0][0]) if result and result[0] and result[0][0] else 0
            
            # 3. Топ-5 компаний (с обработкой пустых значений)
            result = client.execute('''
                SELECT 
                    IF(employer = '' OR employer IS NULL, 'Неизвестно', employer) as employer_name,
                    COUNT(*) as cnt 
                FROM hh_data.vacancies_enhanced 
                GROUP BY employer_name 
                ORDER BY cnt DESC 
                LIMIT 5
            ''')
            stats["top_companies"] = [
                {"name": row[0], "count": row[1]} 
                for row in result
            ]
            
            # 4. Последние 5 вакансий (с обработкой пустых значений)
            result = client.execute('''
                SELECT 
                    name, 
                    IF(employer = '' OR employer IS NULL, 'Неизвестно', employer) as employer,
                    city, 
                    salary_from, 
                    salary_to, 
                    salary_currency,
                    created_at 
                FROM hh_data.vacancies_enhanced 
                ORDER BY created_at DESC 
                LIMIT 5
            ''')
            stats["recent_vacancies"] = []
            for row in result:
                vacancy = {
                    "title": row[0] if row[0] else "Без названия",
                    "company": row[1] if row[1] else "Неизвестно",
                    "city": row[2] if row[2] else "Не указан",
                    "salary_from": int(row[3]) if row[3] is not None else 0,
                    "salary_to": int(row[4]) if row[4] is not None else 0,
                    "currency": row[5] if row[5] else "RUR",
                    "published_date": row[6].strftime('%Y-%m-%d %H:%M:%S') if row[6] else "N/A"
                }
                stats["recent_vacancies"].append(vacancy)
            
            # 5. Время последнего обновления (используем created_at)
            result = client.execute('SELECT MAX(created_at) FROM hh_data.vacancies_enhanced')
            last_update_time = result[0][0] if result and result[0] else None
            stats["last_update"] = last_update_time.strftime('%Y-%m-%d %H:%M:%S') if last_update_time else "N/A"
            
            # Статус Airflow — если есть данные, считаем его здоровым
            if stats["total_vacancies"] > 0:
                airflow_status = {"status": "healthy"}
            
            print("=== DEBUG INFO ===")
            print(f"Total vacancies: {stats['total_vacancies']}")
            print(f"Average salary: {stats['avg_salary']} ₽")
            print(f"Last update: {stats['last_update']}")
            print(f"Top companies: {stats['top_companies']}")
            print(f"Recent vacancies: {stats['recent_vacancies']}")
            print("===================")
            
        except Exception as e:
            print(f'Query error: {e}')
            import traceback
            traceback.print_exc()
            # Не меняем статус на "error", если данные уже загружены
    
    return render_template('index.html', stats=stats, airflow_status=airflow_status)

@app.route('/analytics')
def analytics():
    client = get_clickhouse_client()
    
    data = {
        "salary_distribution": [],
        "experience_levels": [],
        "schedule_types": [],
        "employment_types": [],
        "top_skills": []
    }
    
    if client:
        try:
            # Распределение по зарплате
            result = client.execute('''
                SELECT 
                    CASE 
                        WHEN (salary_from + salary_to) / 2 < 50000 THEN 'До 50 000 ₽'
                        WHEN (salary_from + salary_to) / 2 < 100000 THEN '50 000 - 100 000 ₽'
                        WHEN (salary_from + salary_to) / 2 < 200000 THEN '100 000 - 200 000 ₽'
                        ELSE 'Более 200 000 ₽'
                    END as salary_range,
                    COUNT(*) as count
                FROM hh_data.vacancies_enhanced 
                WHERE salary_from IS NOT NULL AND salary_to IS NOT NULL AND salary_currency IN ('RUR', 'RUB')
                GROUP BY salary_range
                ORDER BY 
                    CASE salary_range
                        WHEN 'До 50 000 ₽' THEN 1
                        WHEN '50 000 - 100 000 ₽' THEN 2
                        WHEN '100 000 - 200 000 ₽' THEN 3
                        ELSE 4
                    END
            ''')
            data["salary_distribution"] = [{"range": row[0], "count": row[1]} for row in result]
            
            # Уровень опыта
            result = client.execute('''
                SELECT 
                    IF(experience = '' OR experience IS NULL, 'Не указан', experience) as exp_level,
                    COUNT(*) as count
                FROM hh_data.vacancies_enhanced 
                GROUP BY exp_level
                ORDER BY count DESC
            ''')
            data["experience_levels"] = [{"level": row[0], "count": row[1]} for row in result]
            
            # График работы
            result = client.execute('''
                SELECT 
                    IF(schedule = '' OR schedule IS NULL, 'Не указан', schedule) as sched_type,
                    COUNT(*) as count
                FROM hh_data.vacancies_enhanced 
                GROUP BY sched_type
                ORDER BY count DESC
            ''')
            data["schedule_types"] = [{"type": row[0], "count": row[1]} for row in result]
            
            # Тип занятости
            result = client.execute('''
                SELECT 
                    IF(employment = '' OR employment IS NULL, 'Не указан', employment) as emp_type,
                    COUNT(*) as count
                FROM hh_data.vacancies_enhanced 
                GROUP BY emp_type
                ORDER BY count DESC
            ''')
            data["employment_types"] = [{"type": row[0], "count": row[1]} for row in result]
            
        except Exception as e:
            print(f'Analytics query error: {e}')
            import traceback
            traceback.print_exc()
    
    return render_template('analytics.html', data=data)

@app.route('/query')
def query():
    return render_template('query.html')

@app.route('/status')
def status():
    client = get_clickhouse_client()
    
    system_info = {
        "clickhouse_status": "error",
        "database_size": "N/A",
        "table_count": "N/A",
        "airflow_status": "error"
    }
    
    if client:
        try:
            # Статус ClickHouse
            result = client.execute('SELECT 1')
            if result:
                system_info["clickhouse_status"] = "healthy"
            
            # Размер базы данных
            result = client.execute('''
                SELECT formatReadableSize(sum(bytes)) 
                FROM system.parts 
                WHERE database = 'hh_data'
            ''')
            system_info["database_size"] = result[0][0] if result else "N/A"
            
            # Количество таблиц
            result = client.execute('''
                SELECT COUNT(*) 
                FROM system.tables 
                WHERE database = 'hh_data'
            ''')
            system_info["table_count"] = result[0][0] if result else "N/A"
            
            # Статус таблицы
            result = client.execute("SELECT COUNT(*) FROM hh_data.vacancies_enhanced")
            if result and result[0][0] > 0:
                system_info["airflow_status"] = "healthy"
            
        except Exception as e:
            print(f'Status query error: {e}')
            import traceback
            traceback.print_exc()
    
    return render_template('status.html', system_info=system_info)

from flask import request, jsonify

@app.route('/execute_query', methods=['POST'])
def execute_query():
    client = get_clickhouse_client()
    
    if not client:
        return jsonify({
            "success": False,
            "error": "Не удалось подключиться к ClickHouse"
        })
    
    try:
        # Получаем SQL-запрос из запроса
        data = request.get_json()
        sql_query = data.get('query', '').strip()
        
        if not sql_query:
            return jsonify({
                "success": False,
                "error": "SQL-запрос не может быть пустым"
            })
        
        # Выполняем запрос
        result = client.execute(sql_query, with_column_types=True)
        
        # Разделяем данные и типы колонок
        rows = result[0]
        columns = result[1] if len(result) > 1 else []
        
        # Формируем результат
        column_names = [col[0] for col in columns] if columns else []
        
        # Если нет колонок, формируем автоматически
        if not column_names and rows:
            column_names = [f"column_{i+1}" for i in range(len(rows[0]) if rows[0] else 0)]
        
        # Преобразуем результат в список словарей
        result_list = []
        for row in rows:
            row_dict = {}
            for i, col_name in enumerate(column_names):
                value = row[i] if i < len(row) else None
                # Преобразуем дату/время в строку
                if hasattr(value, 'strftime'):
                    value = value.strftime('%Y-%m-%d %H:%M:%S')
                row_dict[col_name] = value
            result_list.append(row_dict)
        
        return jsonify({
            "success": True,
            "query": sql_query,
            "columns": column_names,
            "rows": result_list,
            "row_count": len(result_list)
        })
        
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        return jsonify({
            "success": False,
            "error": str(e),
            "error_details": error_details
        })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)