from flask import Flask, render_template_string
import os
from clickhouse_driver import Client

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
        print(f'ClickHouse error: {e}')
        return None

@app.route('/')
def index():
    client = get_clickhouse_client()
    total = 0
    avg_salary = 0
    
    if client:
        try:
            # Общее количество вакансий
            result = client.execute('SELECT COUNT(*) FROM vacancies_simple')
            total = result[0][0] if result and result[0] else 0
            
            # Средняя зарплата
            result = client.execute('''
                SELECT ROUND(AVG((salary_from + salary_to) / 2)) 
                FROM vacancies_simple 
                WHERE salary_from > 0 AND salary_to > 0 AND salary_currency = 'RUR'
            ''')
            avg_salary = result[0][0] if result and result[0] else 0
        except Exception as e:
            print(f'Query error: {e}')
    
    # Гарантируем, что значения числовые
    total = int(total) if total is not None else 0
    avg_salary = int(avg_salary) if avg_salary is not None else 0
    
    return render_template_string('''
    <!DOCTYPE html>
    <html lang='ru'>
    <head>
        <meta charset='UTF-8'>
        <meta name='viewport' content='width=device-width, initial-scale=1.0'>
        <title>HH.ru Analytics</title>
        <style>
            body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); margin: 0; padding: 20px; color: #333; min-height: 100vh; }
            .container { max-width: 1000px; margin: 40px auto; background: white; border-radius: 20px; padding: 40px; box-shadow: 0 15px 50px rgba(0,0,0,0.25); text-align: center; }
            h1 { color: #667eea; margin-bottom: 10px; font-size: 2.5rem; }
            .subtitle { color: #777; margin-bottom: 40px; font-size: 1.2rem; }
            .stat-card { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 35px; border-radius: 16px; margin: 20px auto; width: 300px; box-shadow: 0 10px 30px rgba(102,126,234,0.4); }
            .stat-number { font-size: 4rem; font-weight: bold; margin: 15px 0; }
            .stat-label { font-size: 1.4rem; opacity: 0.95; }
            .links { margin-top: 40px; display: flex; justify-content: center; gap: 25px; flex-wrap: wrap; }
            .link-btn { background: rgba(102,126,234,0.15); color: #667eea; padding: 12px 25px; border-radius: 10px; text-decoration: none; font-weight: 600; font-size: 1.1rem; transition: all 0.3s; }
            .link-btn:hover { background: rgba(102,126,234,0.25); transform: translateY(-3px); }
            .footer { margin-top: 50px; color: rgba(255,255,255,0.8); font-size: 0.95rem; }
        </style>
    </head>
    <body>
        <div class='container'>
            <h1>📊 HH.ru Analytics</h1>
            <div class='subtitle'>Дашборд вакансий на базе ClickHouse + Airflow</div>
            
            <div class='stat-card'>
                <div class='stat-label'>Всего вакансий</div>
                <div class='stat-number'>''' + str(total) + '''</div>
            </div>
            
            <div class='stat-card'>
                <div class='stat-label'>Средняя зарплата</div>
                <div class='stat-number'>''' + str(avg_salary) + ''' ₽</div>
            </div>
            
            <div class='links'>
                <a href='http://localhost:8080' class='link-btn' target='_blank'>✈️ Airflow UI</a>
                <a href='http://localhost:8123' class='link-btn' target='_blank'>📊 ClickHouse</a>
            </div>
            
            <div class='footer'>
                <p>Веб-интерфейс запущен успешно | Порт: 5001</p>
            </div>
        </div>
    </body>
    </html>
    ''')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
