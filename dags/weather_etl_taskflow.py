import pendulum
from airflow.decorators import dag, task
import json
import requests
import logging
from transformer import transform_weatherAPI

@dag(
    dag_id='weather_etl_taskflow',
    schedule_interval="0 * * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=['Weather Project']
)
def weather_etl_taskflow():
    
    @task()
    def extract():
        payload = {'Key': '2362fdd1ad18496886b173609252409', 'q': 'Rabat', 'aqi': 'no'}
        response = requests.get("http://api.weatherapi.com/v1/current.json", params=payload)
        return response.json()

    @task()
    def transform(weather_data):
        weather_str = json.dumps(weather_data)
        return transform_weatherAPI(weather_str)

    @task()
    def load(transformed_data):
        logger = logging.getLogger("airflow.task")
        logger.info(transformed_data)
        return "Success"

    # Exécution
    weather_data = extract()
    transformed_data = transform(weather_data)
    load(transformed_data)

# Instanciation
weather_etl_taskflow = weather_etl_taskflow()
