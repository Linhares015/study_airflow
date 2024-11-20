import requests
import logging

from datetime import datetime, timedelta
from airflow.decorators import dag, task

API = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&ids=bitcoin"

@dag(
    dag_id="task_flow_api",
    start_date=datetime(2024, 11, 19),
    schedule_interval="@daily",
    catchup=False,
)
def main():
    
    @task(task_id="extract", retries=2, retry_delay=timedelta(minutes=2))
    def extract_bitcoin():
        return requests.get(API).json()[0]
    
    @task(task_id="transform")
    def transform_bitcoin(response):
        # Corrigir as chaves para refletirem os dados corretos
        return {
            "usd": response["current_price"],
            "change": response["price_change_percentage_24h"]
        }

    @task(task_id="load")
    def load_bitcoin(data):
        logging.info(f"Bitcoin price: {data['usd']}, change: {data['change']}%")

    load_bitcoin(transform_bitcoin(extract_bitcoin()))

main()
