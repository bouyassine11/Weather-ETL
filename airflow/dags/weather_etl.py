from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import sys
import os
import pandas as pd

# Add parent directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from plugins.helpers.weather_etl_utils import extract_weather_data, transform_weather_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
}

# File paths
EXTRACT_CSV = '/opt/airflow/data/weather_data.csv'
EXTRACT_TMP = '/opt/airflow/data/weather_data_extracted.csv'
TRANSFORM_TMP = '/opt/airflow/data/weather_data_transformed.csv'

# Tasks
def extract_task():
    extract_weather_data(EXTRACT_CSV, EXTRACT_TMP)

def transform_task():
    transform_weather_data(EXTRACT_TMP, TRANSFORM_TMP)

def load_data_to_postgres():
    if not os.path.exists(TRANSFORM_TMP):
        raise FileNotFoundError(f"{TRANSFORM_TMP} does not exist. Transform task failed!")

    df = pd.read_csv(TRANSFORM_TMP)
    if df.empty:
        raise ValueError("No data to load!")

    pg_hook = PostgresHook(postgres_conn_id='postgres_weather')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO weather_data 
            (city, date, temperature_celsius, humidity_percent, wind_speed_kmh, condition)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            row['city'],
            row['date'],
            row['temperature_celsius'],
            row['humidity_percent'],
            row['wind_speed_kmh'],
            row['condition']
        ))
    
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Loaded {len(df)} rows into PostgreSQL.")

# DAG definition
with DAG(
    'weather_etl_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract_task,
    )

    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_task,
    )

    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_data_to_postgres,
    )

    extract_data >> transform_data >> load_data
