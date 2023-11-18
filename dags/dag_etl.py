#Dags of the project

from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from etl_project import extract_dataset, transform_dataset, extract_api, transform_api, merge, load, kafka_stream

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 8),  # Update the start date to today or an appropriate date
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'etl_dag',
    default_args=default_args,
    description='Project',
    schedule_interval='@daily',  # Set the schedule interval as per your requirements
) as dag:

    extract_dataset = PythonOperator(
        task_id='extract_dataset',
        python_callable=extract_dataset,
    )

    transform_dataset = PythonOperator(
        task_id='transform_dataset',
        python_callable=transform_dataset,
        )
    
    extract_api = PythonOperator(
        task_id='extract_api',
        python_callable=extract_api,
    )

    transform_api = PythonOperator(
        task_id='transform_api',
        python_callable=transform_api,
        )
    
    merge = PythonOperator(
        task_id='merge',
        python_callable=merge,
    )

    load = PythonOperator(
        task_id='load',
        python_callable=load,
    )
    
    kafka_stream = PythonOperator(
        task_id='kafka_stream',
        python_callable=kafka_stream,
    )
    
    extract_dataset >> transform_dataset >> merge
    extract_api >> transform_api >> merge >> load >> kafka_stream

