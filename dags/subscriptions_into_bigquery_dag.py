from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import random
import time
from google.cloud import bigquery

# Path to the copied JSON key file within the container
credentials_path = '/opt/airflow/dags/nth-glider-362309-ed1a06b4521f.json'


# Function to generate random subscriptions data
def generate_subscriptions(last_run_time, num_subscriptions):
    subscriptions = []
    for _ in range(num_subscriptions):
        start_date = time.strftime("%Y-%m-%d %H:%M:%S",
                                   time.localtime(random.randint(int(last_run_time), int(time.time()))))
        start_timestamp = int(time.mktime(time.strptime(start_date, "%Y-%m-%d %H:%M:%S")))
        expiration_timestamp = start_timestamp + 28 * 24 * 60 * 60  # Adding 28 days in seconds
        expiration_date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(expiration_timestamp))
        subscription = {
            "subscription_id": random.randint(1, 10000),
            "user_id": random.randint(1, 100),
            "subscription_type": random.choice(["basic", "premium"]),
            "start_date": start_date,
            "expiration_date": expiration_date,
        }
        subscriptions.append(subscription)
    return subscriptions


# Function to save data to BigQuery
# Function to save data to BigQuery
def save_to_bigquery(subscriptions):
    client = bigquery.Client.from_service_account_json(credentials_path)

    dataset_id = 'aa'
    table_id = 'subscriptions'
    schema = [
        bigquery.SchemaField('subscription_id', 'INTEGER'),
        bigquery.SchemaField('user_id', 'INTEGER'),
        bigquery.SchemaField('subscription_type', 'STRING'),
        bigquery.SchemaField('start_date', 'DATETIME'),
        bigquery.SchemaField('expiration_date', 'DATETIME'),
    ]
    dataset_ref = client.dataset(dataset_id)
    dataset = bigquery.Dataset(dataset_ref)
    client.create_dataset(dataset, exists_ok=True)  # Create dataset if not exists
    table_ref = dataset_ref.table(table_id)
    table = bigquery.Table(table_ref, schema=schema)
    table = client.create_table(table, exists_ok=True)  # Create table if not exists
    job_config = bigquery.LoadJobConfig(schema=schema, write_disposition='WRITE_APPEND')
    load_job = client.load_table_from_json(subscriptions, table_ref, job_config=job_config)
    load_job.result()

# Function to generate and save subscriptions data to BigQuery
def generate_and_save_subscriptions(num_subscriptions):
    interval_seconds = 30 * 60
    last_run_time = time.time() - interval_seconds
    subscriptions_data = generate_subscriptions(last_run_time, num_subscriptions)
    save_to_bigquery(subscriptions_data)
    print(f"Subscriptions data updated at {time.strftime('%Y-%m-%d %H:%M:%S')}")


# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'subscriptions_into_bigquery',
    default_args=default_args,
    schedule_interval=timedelta(minutes=30),
    catchup=False
)

# Define a single task using the PythonOperator
num_subscriptions_per_task = 2
run_python_task = PythonOperator(
    task_id='generate_and_save_subscriptions',
    python_callable=generate_and_save_subscriptions,
    op_args=[num_subscriptions_per_task],
    dag=dag,
)

run_python_task