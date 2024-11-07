# imports
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

# default arguments
default_args = {
    'owner': 'Tiago Linhares',
    'email_on_failure': False,
    'email_on_retry': False,
    'email': 'teste@gmail.com',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# dag definition
@dag(
    dag_id='first_dag',
    start_date=datetime(2021, 1, 1),
    max_active_runs=1,
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False,
    owner_links={'likedin': 'https://www.linkedin.com/in/tiagolinhares/'},
    tags=['example']
)

def init():

    # task definition
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    # dependencies

    start >> end

# dag instanciation
dag = init()