import requests
import logging

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

API = "https://api.coingecko.com/api/v3/coins/bitcoin/history?date=23-11-2024&localization=false"

# Todo Extract
def extract_bitcoin():
    return requests.get(API).json()["bitcoin"]

# Todo Process
def process_bitcoin(ti):
    response = ti.xcom_pull(task_ids="extract_bitcoin")
    logging.info(response)
    process_data = {"usd": response["usd"], "change": response["usd_24h_change"]}
    ti.xcom_push(key="process_data", value=process_data)

# Todo Store
def store_bitcoin(ti):
    process_data = ti.xcom_pull(key="process_data", task_ids="process_bitcoin")
    logging.info(process_data)

with DAG(
    dag_id="modelo_tradicional",
    start_date=datetime(2024, 11, 19),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
):
    # Task 1
    extract_bitcoin_task = PythonOperator(task_id="extract_bitcoin", python_callable=extract_bitcoin)

    # Task 2
    process_bitcoin_task = PythonOperator(task_id="process_bitcoin", python_callable=process_bitcoin)

    # Task 3
    store_bitcoin_task = PythonOperator(task_id="store_bitcoin", python_callable=store_bitcoin)

    # Task Dependencies
    extract_bitcoin_task >> process_bitcoin_task >> store_bitcoin_task
