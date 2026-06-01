"""
STEP 4: Apache Airflow DAG
This file tells Airflow HOW and WHEN to run our pipeline automatically.

A DAG (Directed Acyclic Graph) is a workflow definition.
Each 'task' in the DAG is one step in the pipeline.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import sys
import os

# Add our scripts folder to Python path so Airflow can find them
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from etl_pipeline import (
    extract_ridership, transform_ridership, validate_data, load_ridership,
    extract_vehicle_locations, transform_vehicle_locations, load_vehicle_locations,
    extract_weather, transform_weather, load_weather
)

# ── Default DAG arguments ──────────────────────────────────
default_args = {
    'owner': 'urbanmove_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ── Define the DAG ─────────────────────────────────────────
with DAG(
    dag_id='urbanmove_daily_pipeline',
    default_args=default_args,
    description='Daily ETL pipeline for UrbanMove transportation data',
    schedule_interval='0 6 * * *',  # Run at 6:00 AM every day
    catchup=False,
    tags=['urbanmove', 'etl', 'transportation']
) as dag:

    # Task 0: Pipeline start marker
    start = EmptyOperator(task_id='pipeline_start')

    # ── Ridership Tasks ──────────────────────────────────
    def run_extract_ridership():
        df = extract_ridership()
        return "extracted"

    def run_transform_load_ridership():
        df = extract_ridership()
        df = transform_ridership(df)
        validate_data(df, 'ridership')
        load_ridership(df)

    extract_ridership_task = PythonOperator(
        task_id='extract_ridership',
        python_callable=run_extract_ridership
    )

    load_ridership_task = PythonOperator(
        task_id='transform_load_ridership',
        python_callable=run_transform_load_ridership
    )

    # ── Vehicle Location Tasks ───────────────────────────
    def run_transform_load_locations():
        df = extract_vehicle_locations()
        df = transform_vehicle_locations(df)
        validate_data(df, 'vehicle_locations')
        load_vehicle_locations(df)

    load_locations_task = PythonOperator(
        task_id='transform_load_vehicle_locations',
        python_callable=run_transform_load_locations
    )

    # ── Weather Tasks ────────────────────────────────────
    def run_transform_load_weather():
        df = extract_weather()
        df = transform_weather(df)
        validate_data(df, 'weather')
        load_weather(df)

    load_weather_task = PythonOperator(
        task_id='transform_load_weather',
        python_callable=run_transform_load_weather
    )

    # Task: Pipeline end marker
    end = EmptyOperator(task_id='pipeline_complete')

    # ── Define task dependencies (the order tasks run) ──
    # start → extract_ridership → load_ridership → end
    #                           → load_locations → end
    #                           → load_weather   → end

    start >> extract_ridership_task
    extract_ridership_task >> [load_ridership_task, load_locations_task, load_weather_task]
    [load_ridership_task, load_locations_task, load_weather_task] >> end