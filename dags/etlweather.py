from airflow import DAG
from datetime import datetime
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task




LATITUDE = '40.7128f2'
LONGITUDE = '-0.1278'
POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_meteo_api'
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['kulalraghavendra4@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'start_date': datetime(2025, 1, 1),
    'retries': 1
}

with DAG(
    dag_id='weather_etl_pipeline',
    default_args=default_args,
    schedule='@daily',
    catchup=False) as dag:

    @task()
    def extract_weather_data():
        """Extract weather date from api"""
        #use http hook to get weather data
        http_hook = HttpHook(http_conn_id=API_CONN_ID,method="GET")
        #build api end point
        ##https://api.open-meteo.com/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true
        endpoint = f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'
        response = http_hook.run(endpoint)

        if response.status_code == 200:
            print(response.json())
        else:
            raise Exception(f'Error getting weather data: {response.status_code}')


    weather_data = extract_weather_data()

