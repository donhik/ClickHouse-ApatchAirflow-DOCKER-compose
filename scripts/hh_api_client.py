"""
Модуль для работы с API hh.ru
"""

import requests
import time
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import pandas as pd

# Настройка логгирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class Vacancy:
    """Класс для хранения данных вакансии"""
    vacancy_id: str
    name: str
    area_name: str
    salary_from: Optional[int]
    salary_to: Optional[int]
    salary_currency: Optional[str]
    salary_gross: Optional[bool]
    employer_name: str
    employer_id: str
    published_at: str
    created_at: str
    experience_name: str
    employment_name: str
    schedule_name: str
    key_skills: List[str]
    description: str
    url: str
    address: Optional[str]
    metro_stations: List[Dict]
    
    @classmethod
    def from_api_data(cls, data: Dict) -> 'Vacancy':
        """Создает объект Vacancy из данных API"""
        salary = data.get('salary')
        
        # Обработка ключевых навыков
        key_skills = []
        if data.get('key_skills'):
            key_skills = [skill['name'] for skill in data['key_skills']]
        
        # Обработка станций метро
        metro_stations = []
        if data.get('address') and data['address'].get('metro_stations'):
            metro_stations = [
                {
                    'station_name': metro.get('station_name'),
                    'line_name': metro.get('line_name'),
                    'station_id': metro.get('station_id'),
                    'lat': metro.get('lat'),
                    'lng': metro.get('lng')
                }
                for metro in data['address']['metro_stations']
            ]
        
        return cls(
            vacancy_id=data['id'],
            name=data['name'],
            area_name=data['area']['name'] if data.get('area') else None,
            salary_from=salary.get('from') if salary else None,
            salary_to=salary.get('to') if salary else None,
            salary_currency=salary.get('currency') if salary else None,
            salary_gross=salary.get('gross') if salary else None,
            employer_name=data['employer']['name'] if data.get('employer') else None,
            employer_id=data['employer']['id'] if data.get('employer') else None,
            published_at=data.get('published_at'),
            created_at=data.get('created_at'),
            experience_name=data['experience']['name'] if data.get('experience') else None,
            employment_name=data['employment']['name'] if data.get('employment') else None,
            schedule_name=data['schedule']['name'] if data.get('schedule') else None,
            key_skills=key_skills,
            description=data.get('description', '')[:10000],  # Ограничиваем длину
            url=data.get('alternate_url') or f"https://hh.ru/vacancy/{data['id']}",
            address=data['address']['raw'] if data.get('address') else None,
            metro_stations=metro_stations
        )


class HHAPIClient:
    """Клиент для работы с API hh.ru"""
    
    def __init__(self, config_path: str = 'config/hh_config.json'):
        self.config = self._load_config(config_path)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': self.config['hh_api']['user_agent'],
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })
        self.base_url = self.config['hh_api']['base_url']
        
        # Добавляем авторизацию если есть токен
        if self.config['hh_api'].get('access_token'):
            self.session.headers['Authorization'] = f"Bearer {self.config['hh_api']['access_token']}"
    
    def _load_config(self, config_path: str) -> Dict:
        """Загружает конфигурацию из JSON файла"""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning(f"Config file {config_path} not found, using defaults")
            return {
                'hh_api': {
                    'base_url': 'https://api.hh.ru',
                    'user_agent': 'MyApp/1.0 (my-app-feedback@example.com)'
                }
            }
    
    def search_vacancies(
        self,
        text: str = 'python',
        area: int = 1,  # Москва
        period: int = 30,
        per_page: int = 100,
        pages: int = 20
    ) -> List[Vacancy]:
        """
        Поиск вакансий по параметрам
        
        Args:
            text: Текст для поиска
            area: ID региона (1 - Москва, 2 - СПб)
            period: За какой период искать (дни)
            per_page: Количество результатов на странице
            pages: Количество страниц для получения
        
        Returns:
            List[Vacancy]: Список вакансий
        """
        vacancies = []
        params = {
            'text': text,
            'area': area,
            'period': period,
            'per_page': per_page,
            'page': 0
        }
        
        try:
            for page in range(pages):
                params['page'] = page
                logger.info(f"Запрос страницы {page + 1}/{pages}")
                
                response = self.session.get(
                    f"{self.base_url}/vacancies",
                    params=params,
                    timeout=30
                )
                
                if response.status_code != 200:
                    logger.error(f"Ошибка API: {response.status_code}")
                    break
                
                data = response.json()
                
                # Проверяем есть ли вакансии
                if not data.get('items'):
                    logger.info("Больше нет вакансий")
                    break
                
                # Получаем детальную информацию по каждой вакансии
                for item in data['items']:
                    try:
                        vacancy = self.get_vacancy_details(item['id'])
                        if vacancy:
                            vacancies.append(vacancy)
                    except Exception as e:
                        logger.error(f"Ошибка получения вакансии {item['id']}: {e}")
                
                # Соблюдаем лимиты API (не более 5 запросов в секунду)
                time.sleep(0.3)
                
                # Проверяем последняя ли это страница
                if page >= data.get('pages', 0) - 1:
                    logger.info("Достигнута последняя страница")
                    break
            
            logger.info(f"Найдено {len(vacancies)} вакансий")
            return vacancies
            
        except requests.RequestException as e:
            logger.error(f"Ошибка сети: {e}")
            return []
        except Exception as e:
            logger.error(f"Неизвестная ошибка: {e}")
            return []
    
    def get_vacancy_details(self, vacancy_id: str) -> Optional[Vacancy]:
        """
        Получает детальную информацию о вакансии
        
        Args:
            vacancy_id: ID вакансии
        
        Returns:
            Optional[Vacancy]: Объект вакансии или None
        """
        try:
            response = self.session.get(
                f"{self.base_url}/vacancies/{vacancy_id}",
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                return Vacancy.from_api_data(data)
            elif response.status_code == 404:
                logger.warning(f"Вакансия {vacancy_id} не найдена")
            else:
                logger.error(f"Ошибка API для вакансии {vacancy_id}: {response.status_code}")
            
            return None
            
        except requests.RequestException as e:
            logger.error(f"Ошибка сети при получении вакансии {vacancy_id}: {e}")
            return None
        except Exception as e:
            logger.error(f"Ошибка при обработке вакансии {vacancy_id}: {e}")
            return None
    
    def get_areas(self) -> List[Dict]:
        """Получает список регионов"""
        try:
            response = self.session.get(f"{self.base_url}/areas", timeout=10)
            if response.status_code == 200:
                return response.json()
            return []
        except Exception as e:
            logger.error(f"Ошибка получения регионов: {e}")
            return []
    
    def get_industries(self) -> List[Dict]:
        """Получает список индустрий"""
        try:
            response = self.session.get(f"{self.base_url}/industries", timeout=10)
            if response.status_code == 200:
                return response.json()
            return []
        except Exception as e:
            logger.error(f"Ошибка получения индустрий: {e}")
            return []
    
    def save_vacancies_to_json(self, vacancies: List[Vacancy], filename: str):
        """Сохраняет вакансии в JSON файл"""
        try:
            data = []
            for vacancy in vacancies:
                vacancy_dict = {
                    'vacancy_id': vacancy.vacancy_id,
                    'name': vacancy.name,
                    'area_name': vacancy.area_name,
                    'salary_from': vacancy.salary_from,
                    'salary_to': vacancy.salary_to,
                    'salary_currency': vacancy.salary_currency,
                    'salary_gross': vacancy.salary_gross,
                    'employer_name': vacancy.employer_name,
                    'employer_id': vacancy.employer_id,
                    'published_at': vacancy.published_at,
                    'created_at': vacancy.created_at,
                    'experience_name': vacancy.experience_name,
                    'employment_name': vacancy.employment_name,
                    'schedule_name': vacancy.schedule_name,
                    'key_skills': vacancy.key_skills,
                    'description': vacancy.description[:500],  # Сохраняем только начало
                    'url': vacancy.url,
                    'address': vacancy.address,
                    'metro_stations': vacancy.metro_stations,
                    'loaded_at': datetime.now().isoformat()
                }
                data.append(vacancy_dict)
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Данные сохранены в {filename}")
            
        except Exception as e:
            logger.error(f"Ошибка сохранения в JSON: {e}")
    
    def vacancies_to_dataframe(self, vacancies: List[Vacancy]) -> pd.DataFrame:
        """Конвертирует список вакансий в DataFrame"""
        data = []
        for vacancy in vacancies:
            row = {
                'vacancy_id': vacancy.vacancy_id,
                'name': vacancy.name,
                'area_name': vacancy.area_name,
                'salary_from': vacancy.salary_from,
                'salary_to': vacancy.salary_to,
                'salary_currency': vacancy.salary_currency,
                'salary_gross': vacancy.salary_gross,
                'employer_name': vacancy.employer_name,
                'employer_id': vacancy.employer_id,
                'published_at': vacancy.published_at,
                'created_at': vacancy.created_at,
                'experience_name': vacancy.experience_name,
                'employment_name': vacancy.employment_name,
                'schedule_name': vacancy.schedule_name,
                'key_skills': ', '.join(vacancy.key_skills) if vacancy.key_skills else None,
                'key_skills_count': len(vacancy.key_skills),
                'description_length': len(vacancy.description),
                'url': vacancy.url,
                'address': vacancy.address,
                'metro_stations_count': len(vacancy.metro_stations),
                'loaded_at': datetime.now().isoformat()
            }
            data.append(row)
        
        return pd.DataFrame(data)


class ClickHouseLoader:
    """Класс для загрузки данных в ClickHouse"""
    
    def __init__(self, config: Dict):
        self.config = config['clickhouse']
        
    def connect(self):
        """Создает подключение к ClickHouse"""
        try:
            from clickhouse_driver import Client
            
            self.client = Client(
                host=self.config['host'],
                port=self.config['port'],
                user=self.config['user'],
                password=self.config['password'],
                database=self.config['database'],
                settings={'use_numpy': True}
            )
            logger.info("Подключение к ClickHouse установлено")
            return True
        except Exception as e:
            logger.error(f"Ошибка подключения к ClickHouse: {e}")
            return False
    
    def create_tables(self):
        """Создает таблицы в ClickHouse если их нет"""
        try:
            # Создаем базу данных если нет
            self.client.execute(f"CREATE DATABASE IF NOT EXISTS {self.config['database']}")
            
            # Таблица для вакансий
            vacancies_table = f'''
            CREATE TABLE IF NOT EXISTS {self.config['database']}.hh_vacancies
            (
                vacancy_id String,
                name String,
                area_name Nullable(String),
                salary_from Nullable(Int64),
                salary_to Nullable(Int64),
                salary_currency Nullable(String),
                salary_gross Nullable(Bool),
                employer_name Nullable(String),
                employer_id String,
                published_at DateTime,
                created_at DateTime,
                experience_name Nullable(String),
                employment_name Nullable(String),
                schedule_name Nullable(String),
                key_skills Array(String),
                description String,
                url String,
                address Nullable(String),
                metro_stations String,
                loaded_at DateTime DEFAULT now()
            )
            ENGINE = MergeTree()
            ORDER BY (published_at, area_name, employer_id)
            PARTITION BY toYYYYMM(published_at)
            '''
            
            self.client.execute(vacancies_table)
            logger.info("Таблица hh_vacancies создана или уже существует")
            
            # Таблица для статистики
            stats_table = f'''
            CREATE TABLE IF NOT EXISTS {self.config['database']}.hh_stats_daily
            (
                date Date,
                area_name String,
                experience_name String,
                employment_name String,
                vacancies_count UInt64,
                avg_salary_from Float64,
                avg_salary_to Float64,
                total_vacancies UInt64
            )
            ENGINE = SummingMergeTree()
            ORDER BY (date, area_name, experience_name, employment_name)
            PARTITION BY toYYYYMM(date)
            '''
            
            self.client.execute(stats_table)
            logger.info("Таблица hh_stats_daily создана или уже существует")
            
            return True
            
        except Exception as e:
            logger.error(f"Ошибка создания таблиц: {e}")
            return False
    
    def load_vacancies(self, vacancies: List[Vacancy]):
        """Загружает вакансии в ClickHouse"""
        try:
            data = []
            for vacancy in vacancies:
                # Конвертируем метро станции в JSON строку
                metro_json = json.dumps(vacancy.metro_stations, ensure_ascii=False)
                
                row = (
                    vacancy.vacancy_id,
                    vacancy.name,
                    vacancy.area_name,
                    vacancy.salary_from,
                    vacancy.salary_to,
                    vacancy.salary_currency,
                    vacancy.salary_gross,
                    vacancy.employer_name,
                    vacancy.employer_id,
                    vacancy.published_at,
                    vacancy.created_at,
                    vacancy.experience_name,
                    vacancy.employment_name,
                    vacancy.schedule_name,
                    vacancy.key_skills,
                    vacancy.description[:10000],  # Ограничиваем для ClickHouse
                    vacancy.url,
                    vacancy.address,
                    metro_json
                )
                data.append(row)
            
            if data:
                query = f'''
                INSERT INTO {self.config['database']}.hh_vacancies 
                (vacancy_id, name, area_name, salary_from, salary_to, salary_currency, 
                 salary_gross, employer_name, employer_id, published_at, created_at,
                 experience_name, employment_name, schedule_name, key_skills, description,
                 url, address, metro_stations)
                VALUES
                '''
                
                self.client.execute(query, data)
                logger.info(f"Загружено {len(data)} вакансий в ClickHouse")
                return True
            else:
                logger.warning("Нет данных для загрузки")
                return False
                
        except Exception as e:
            logger.error(f"Ошибка загрузки в ClickHouse: {e}")
            return False
    
    def update_daily_stats(self):
        """Обновляет ежедневную статистику"""
        try:
            query = f'''
            INSERT INTO {self.config['database']}.hh_stats_daily
            SELECT
                toDate(published_at) as date,
                area_name,
                experience_name,
                employment_name,
                count() as vacancies_count,
                avg(salary_from) as avg_salary_from,
                avg(salary_to) as avg_salary_to,
                count() as total_vacancies
            FROM {self.config['database']}.hh_vacancies
            WHERE published_at >= today() - 30
            GROUP BY date, area_name, experience_name, employment_name
            '''
            
            self.client.execute(query)
            logger.info("Ежедневная статистика обновлена")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка обновления статистики: {e}")
            return False


def main():
    """Основная функция для тестирования"""
    # Инициализация клиента
    client = HHAPIClient()
    
    # Получаем вакансии
    vacancies = client.search_vacancies(
        text='python developer',
        area=1,
        period=7,
        per_page=50,
        pages=5
    )
    
    if vacancies:
        # Сохраняем в JSON
        client.save_vacancies_to_json(vacancies, 'data/vacancies.json')
        
        # Конвертируем в DataFrame
        df = client.vacancies_to_dataframe(vacancies)
        print(f"Получено {len(df)} вакансий")
        print(df.head())
        
        # Загружаем в ClickHouse
        loader = ClickHouseLoader(client.config)
        if loader.connect():
            loader.create_tables()
            loader.load_vacancies(vacancies)
            loader.update_daily_stats()
    
    return vacancies


if __name__ == "__main__":
    main()
