from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 21)
}

dag = DAG(
    'test_airflow1',
    default_args=default_args,
    schedule_interval=None
)

check_airflow_task = BashOperator(
    task_id='check_airflow',
    bash_command='echo "Airflow is jonzing"',
    dag=dag
)

check_airflow_task
