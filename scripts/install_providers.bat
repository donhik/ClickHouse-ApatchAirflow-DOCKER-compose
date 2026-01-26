@echo off
echo Установка ClickHouse provider для AirFlow...

REM Получаем ID контейнера AirFlow worker
for /f "tokens=*" %%i in ('docker ps -q -f "name=airflow-worker"') do set CONTAINER_ID=%%i

if "%CONTAINER_ID%"=="" (
    echo Контейнер airflow-worker не найден!
    exit /b 1
)

echo Установка в контейнер %CONTAINER_ID%...
docker exec %CONTAINER_ID% pip install apache-airflow-providers-clickhouse

echo Установка завершена!
echo Перезапустите AirFlow worker для применения изменений:
echo docker restart airflow-worker
