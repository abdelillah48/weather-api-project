import pendulum
from airflow.decorators import dag, task
import json
import requests
import psycopg2
from transformer import transform_weatherAPI

@dag(
    dag_id='weather_etl_postgres',
    schedule_interval="0 * * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=['Weather Project']
)
def weather_etl():
    
    @task()
    def extract():
        payload = {'Key': '2362fdd1ad18496886b173609252409', 'q': 'Rabat', 'aqi': 'no'}
        response = requests.get("http://api.weatherapi.com/v1/current.json", params=payload)
        return response.json()

    @task()
    def transform(weather_data):
        weather_str = json.dumps(weather_data)
        transformed_str = transform_weatherAPI(weather_str)
        return json.loads(transformed_str)

    @task()
    def load(transformed_data):
        try:
            connection = psycopg2.connect(
                user="abdel",
                password="abdel",
                host="postgres",
                port="5432",
                database="RabatWeather"
            )
            cursor = connection.cursor()

            postgres_insert_query = """
                INSERT INTO weather_data 
                (location, temp_c, wind_kph, timestamp)
                VALUES (%s, %s, %s, %s)
            """
            
            record_to_insert = (
                transformed_data[0]["location"], 
                transformed_data[0]["temp_c"], 
                transformed_data[0]["wind_kph"], 
                transformed_data[0]["timestamp"]
            )
            
            cursor.execute(postgres_insert_query, record_to_insert)
            connection.commit()
            print(cursor.rowcount, "Record inserted successfully")

        except (Exception, psycopg2.Error) as error:
            print("Failed to insert record", error)
            raise Exception(error)
            
        finally:
            if connection:
                cursor.close()
                connection.close()

        return "Success"

    # Exécution
    weather_data = extract()
    transformed_data = transform(weather_data)
    load(transformed_data)

# Instanciation
weather_etl_dag = weather_etl()
