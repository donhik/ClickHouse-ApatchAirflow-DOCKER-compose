#!/usr/bin/env python3
"""
Скрипт для тестирования работы с API hh.ru
"""

import requests
import json
import time
from datetime import datetime

def test_hh_api():
    """Тестирует подключение к API hh.ru"""
    print("=== ТЕСТ ПОДКЛЮЧЕНИЯ К HH.RU API ===")
    
    url = "https://api.hh.ru/vacancies"
    params = {'text': 'python', 'area': 1, 'per_page': 1}
    headers = {'User-Agent': 'TestApp/1.0'}
    
    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ API доступен")
            print(f"   Найдено вакансий: {data.get('found', 0)}")
            print(f"   Страниц: {data.get('pages', 0)}")
            return True
        else:
            print(f"❌ Ошибка API: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Ошибка подключения: {e}")
        return False

def test_clickhouse():
    """Тестирует подключение к ClickHouse"""
    print("\n=== ТЕСТ ПОДКЛЮЧЕНИЯ К CLICKHOUSE ===")
    
    try:
        from clickhouse_driver import Client
        
        client = Client(
            host='localhost',
            port=9000,
            user='admin',
            password='clickhouse_pass'
        )
        
        result = client.execute('SELECT 1 as test, version() as version')
        print(f"✅ ClickHouse доступен")
        print(f"   Версия: {result[0][1]}")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка ClickHouse: {e}")
        return False

def fetch_sample_vacancies():
    """Получает несколько вакансий для примера"""
    print("\n=== ПОЛУЧЕНИЕ ПРИМЕРНЫХ ВАКАНСИЙ ===")
    
    url = "https://api.hh.ru/vacancies"
    params = {
        'text': 'python developer',
        'area': 1,
        'per_page': 5,
        'page': 0
    }
    headers = {'User-Agent': 'TestApp/1.0'}
    
    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            vacancies = data.get('items', [])
            
            print(f"Получено {len(vacancies)} вакансий:")
            
            for i, vacancy in enumerate(vacancies[:3], 1):
                print(f"\n{i}. {vacancy.get('name')}")
                print(f"   Компания: {vacancy.get('employer', {}).get('name')}")
                salary = vacancy.get('salary')
                if salary:
                    salary_str = f"{salary.get('from') or '?'}-{salary.get('to') or '?'} {salary.get('currency')}"
                    print(f"   Зарплата: {salary_str}")
                print(f"   URL: {vacancy.get('alternate_url')}")
            
            # Сохраняем в файл
            filename = f'hh_vacancies_sample_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(vacancies, f, ensure_ascii=False, indent=2)
            
            print(f"\n✅ Данные сохранены в {filename}")
            return True
            
        else:
            print(f"❌ Ошибка: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return False

def main():
    """Основная функция"""
    print("=" * 60)
    print("ТЕСТИРОВАНИЕ HH.RU API И CLICKHOUSE")
    print("=" * 60)
    
    # Тестируем API
    api_ok = test_hh_api()
    
    # Тестируем ClickHouse
    ch_ok = test_clickhouse()
    
    # Если оба сервиса работают, получаем данные
    if api_ok and ch_ok:
        fetch_sample_vacancies()
    else:
        print("\n⚠️  Не все сервисы доступны, проверьте подключение")
    
    print("\n" + "=" * 60)
    print("ТЕСТИРОВАНИЕ ЗАВЕРШЕНО")
    print("=" * 60)

if __name__ == "__main__":
    main()
