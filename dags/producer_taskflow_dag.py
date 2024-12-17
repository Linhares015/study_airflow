from airflow.decorators import dag, task
from airflow.datasets import Dataset
from datetime import datetime

# Define o Dataset
my_dataset = Dataset("/tmp/my_data.csv")

# Define o DAG que publica o Dataset
@dag(
    dag_id="producer_taskflow_dag",
    start_date=datetime(2024, 6, 10),
    schedule_interval="@daily",
    catchup=False,
)
def producer_dag():

    @task(outlets=[my_dataset])  # Define o Dataset como saída
    def generate_data():
        with open("/tmp/my_data.csv", "w") as f:
            f.write("id,name,value\n1,John,100\n2,Jane,200")
        print("Dataset atualizado!")

    generate_data()

producer_dag()
