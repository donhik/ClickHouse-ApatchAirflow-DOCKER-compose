"""
DAG для сбора данных с hh.ru и загрузки в ClickHouse
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
import sys
import os

# Добавляем путь к нашим скриптам
sys.path.insert(0, '/opt/airflow/scripts')

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
}

with DAG(
    'hh_ru_vacancies_pipeline',
    default_args=default_args,
    description='Сбор вакансий с hh.ru и загрузка в ClickHouse',
    schedule_interval='0 2 * * *',  # Каждый день в 2:00
    catchup=False,
    max_active_runs=1,
    tags=['hh.ru', 'vacancies', 'clickhouse', 'etl']
) as dag:
    
    start = DummyOperator(task_id='start')
    
    @task(task_id='load_hh_api_module')
    def load_hh_api_module(**context):
        """Загружает модуль для работы с API hh.ru"""
        import sys
        sys.path.insert(0, '/opt/airflow/scripts')
        
        try:
            from hh_api_client import HHAPIClient, ClickHouseLoader
            context['ti'].xcom_push(key='modules_loaded', value=True)
            return "Модули загружены успешно"
        except Exception as e:
            raise Exception(f"Ошибка загрузки модулей: {e}")
    
    @task(task_id='fetch_vacancies')
    def fetch_vacancies(**context):
        """Получает вакансии с hh.ru"""
        import json
        from datetime import datetime
        
        try:
            from hh_api_client import HHAPIClient
            
            # Загружаем конфигурацию
            config_path = '/opt/airflow/config/hh_config.json'
            client = HHAPIClient(config_path)
            
            # Параметры поиска (можно вынести в Variables)
            search_params = {
                'text': 'python developer',
                'area': 1,  # Москва
                'period': 1,  # За последний день
                'per_page': 100,
                'pages': 10
            }
            
            # Получаем вакансии
            vacancies = client.search_vacancies(**search_params)
            
            # Сохраняем информацию о количестве
            context['ti'].xcom_push(key='vacancies_count', value=len(vacancies))
            
            # Сохраняем в временный файл
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'/tmp/hh_vacancies_{timestamp}.json'
            client.save_vacancies_to_json(vacancies, filename)
            
            # Возвращаем путь к файлу
            context['ti'].xcom_push(key='vacancies_file', value=filename)
            
            print(f"Получено {len(vacancies)} вакансий")
            print(f"Сохранено в файл: {filename}")
            
            return f"Получено {len(vacancies)} вакансий"
            
        except Exception as e:
            raise Exception(f"Ошибка получения вакансий: {e}")
    
    @task(task_id='load_to_clickhouse')
    def load_to_clickhouse(**context):
        """Загружает данные в ClickHouse"""
        import json
        from datetime import datetime
        
        try:
            from hh_api_client import HHAPIClient, ClickHouseLoader
            from clickhouse_driver import Client
            
            # Получаем путь к файлу из предыдущей задачи
            ti = context['ti']
            vacancies_file = ti.xcom_pull(task_ids='fetch_vacancies', key='vacancies_file')
            
            if not vacancies_file:
                raise Exception("Файл с вакансиями не найден")
            
            # Загружаем данные из файла
            with open(vacancies_file, 'r', encoding='utf-8') as f:
                vacancies_data = json.load(f)
            
            # Конвертируем в объекты Vacancy
            from hh_api_client import Vacancy
            
            vacancies = []
            for item in vacancies_data:
                try:
                    # Создаем словарь в формате API
                    api_data = {
                        'id': item['vacancy_id'],
                        'name': item['name'],
                        'area': {'name': item['area_name']} if item['area_name'] else None,
                        'salary': {
                            'from': item['salary_from'],
                            'to': item['salary_to'],
                            'currency': item['salary_currency'],
                            'gross': item['salary_gross']
                        } if item['salary_from'] or item['salary_to'] else None,
                        'employer': {
                            'name': item['employer_name'],
                            'id': item['employer_id']
                        } if item['employer_name'] else None,
                        'published_at': item['published_at'],
                        'created_at': item['created_at'],
                        'experience': {'name': item['experience_name']} if item['experience_name'] else None,
                        'employment': {'name': item['employment_name']} if item['employment_name'] else None,
                        'schedule': {'name': item['schedule_name']} if item['schedule_name'] else None,
                        'key_skills': [{'name': skill} for skill in item['key_skills']],
                        'description': item.get('description', ''),
                        'alternate_url': item['url'],
                        'address': {'raw': item['address']} if item['address'] else None
                    }
                    
                    # Добавляем метро станции
                    if item.get('metro_stations'):
                        api_data['address']['metro_stations'] = item['metro_stations']
                    
                    vacancy = Vacancy.from_api_data(api_data)
                    vacancies.append(vacancy)
                    
                except Exception as e:
                    print(f"Ошибка конвертации вакансии: {e}")
                    continue
            
            # Загружаем в ClickHouse
            config_path = '/opt/airflow/config/hh_config.json'
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            loader = ClickHouseLoader(config)
            
            if loader.connect():
                loader.create_tables()
                success = loader.load_vacancies(vacancies)
                
                if success:
                    loader.update_daily_stats()
                    return f"Успешно загружено {len(vacancies)} вакансий в ClickHouse"
                else:
                    raise Exception("Ошибка загрузки в ClickHouse")
            else:
                raise Exception("Не удалось подключиться к ClickHouse")
            
        except Exception as e:
            raise Exception(f"Ошибка загрузки в ClickHouse: {e}")
    
    @task(task_id='generate_report')
    def generate_report(**context):
        """Генерирует отчет по собранным данным"""
        from datetime import datetime
        import json
        
        try:
            from clickhouse_driver import Client
            
            # Получаем количество вакансий
            ti = context['ti']
            vacancies_count = ti.xcom_pull(task_ids='fetch_vacancies', key='vacancies_count')
            
            # Подключаемся к ClickHouse
            config_path = '/opt/airflow/config/hh_config.json'
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            ch_config = config['clickhouse']
            client = Client(
                host=ch_config['host'],
                port=ch_config['port'],
                user=ch_config['user'],
                password=ch_config['password'],
                database=ch_config['database']
            )
            
            # Получаем статистику
            stats_query = f"""
            SELECT 
                count() as total_vacancies,
                uniq(employer_id) as unique_employers,
                avg(salary_from) as avg_salary_from,
                avg(salary_to) as avg_salary_to,
                countIf(salary_currency = 'RUR') as rub_vacancies,
                countIf(experience_name = 'Нет опыта') as no_experience,
                countIf(experience_name = 'От 1 года до 3 лет') as mid_experience,
                countIf(experience_name = 'Более 6 лет') as senior_experience
            FROM {ch_config['database']}.hh_vacancies
            WHERE published_at >= today() - 30
            """
            
            stats = client.execute(stats_query)
            
            if stats:
                total, employers, avg_from, avg_to, rub, no_exp, mid_exp, senior_exp = stats[0]
                
                report = f"""
                ========================================
                ОТЧЕТ ПО ВАКАНСИЯМ HH.RU
                Дата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                ========================================
                
                📊 СТАТИСТИКА ЗА ПОСЛЕДНИЕ 30 ДНЕЙ:
                
                Всего вакансий: {total}
                Уникальных работодателей: {employers}
                
                💰 ЗАРПЛАТЫ:
                Средняя зарплата от: {avg_from:.0f} руб.
                Средняя зарплата до: {avg_to:.0f} руб.
                Вакансий в рублях: {rub}
                
                👨‍💻 ОПЫТ РАБОТЫ:
                Без опыта: {no_exp}
                1-3 года опыта: {mid_exp}
                Более 6 лет: {senior_exp}
                
                🎯 СЕГОДНЯШНИЙ СБОР:
                Новых вакансий: {vacancies_count or 0}
                
                ========================================
                """
                
                print(report)
                
                # Сохраняем отчет в файл
                report_file = f"/tmp/hh_report_{datetime.now().strftime('%Y%m%d')}.txt"
                with open(report_file, 'w', encoding='utf-8') as f:
                    f.write(report)
                
                print(f"Отчет сохранен: {report_file}")
                
                return report
            else:
                return "Нет данных для отчета"
                
        except Exception as e:
            print(f"Ошибка генерации отчета: {e}")
            return f"Ошибка отчета: {e}"
    
    @task(task_id='cleanup_temp_files')
    def cleanup_temp_files(**context):
        """Очищает временные файлы"""
        import os
        import glob
        
        try:
            # Удаляем временные файлы с вакансиями
            temp_files = glob.glob('/tmp/hh_vacancies_*.json')
            for file in temp_files:
                try:
                    os.remove(file)
                    print(f"Удален файл: {file}")
                except Exception as e:
                    print(f"Ошибка удаления файла {file}: {e}")
            
            # Удаляем старые отчеты (старше 7 дней)
            import datetime
            report_files = glob.glob('/tmp/hh_report_*.txt')
            for file in report_files:
                try:
                    file_time = os.path.getmtime(file)
                    if datetime.datetime.now().timestamp() - file_time > 7 * 24 * 3600:
                        os.remove(file)
                        print(f"Удален старый отчет: {file}")
                except Exception as e:
                    print(f"Ошибка проверки файла {file}: {e}")
            
            return "Временные файлы очищены"
            
        except Exception as e:
            print(f"Ошибка очистки файлов: {e}")
            return f"Ошибка очистки: {e}"
    
    end = DummyOperator(task_id='end')
    
    # Определяем порядок выполнения
    start >> load_hh_api_module() >> fetch_vacancies() >> load_to_clickhouse()
    load_to_clickhouse() >> generate_report() >> cleanup_temp_files() >> end
    
    # Альтернативный путь при ошибке
    from airflow.operators.python import ShortCircuitOperator
    
    @task(task_id='check_vacancies_count')
    def check_vacancies_count(**context):
        """Проверяет количество вакансий перед загрузкой в ClickHouse"""
        ti = context['ti']
        count = ti.xcom_pull(task_ids='fetch_vacancies', key='vacancies_count')
        
        if count and count > 0:
            print(f"Найдено {count} вакансий, продолжаем")
            return True
        else:
            print("Вакансий не найдено, пропускаем загрузку в ClickHouse")
            return False
    
    # Добавляем проверку
    check_task = check_vacancies_count()
    
    # Обновляем порядок
    start >> load_hh_api_module() >> fetch_vacancies() >> check_task
    check_task >> load_to_clickhouse() >> generate_report() >> cleanup_temp_files() >> end
    
    # Если вакансий нет, сразу идем на генерацию отчета
    check_task >> generate_report() >> cleanup_temp_files() >> end
