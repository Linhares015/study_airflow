from airflow.decorators import dag, task
from airflow.datasets import Dataset
from datetime import datetime

# Define o mesmo Dataset como entrada
my_dataset = Dataset("/tmp/my_data.csv")

# Define o DAG que consome o Dataset
@dag(
    dag_id="consumer_taskflow_dag",
    start_date=datetime(2024, 6, 10),
    schedule=[my_dataset],  # Define o Dataset como gatilho
    catchup=False,
)
def consumer_dag():

    @task
    def process_data():
        with open("/tmp/my_data.csv", "r") as f:
            data = f.readlines()
            print("Dados consumidos do Dataset:")
            for line in data:
                print(line.strip())

    process_data()

consumer_dag()
