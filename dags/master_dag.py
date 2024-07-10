from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 2, 2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'crypto_price_pipeline',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False
)

read_files = SparkSubmitOperator(
    task_id='read_files',
    application='/opt/airflow/scripts/read_files.py',
    name='read_files',
    application_args=['/opt/airflow/data/raw/', '/opt/airflow/data/standard_raw/'],
    executor_memory='2g',
    executor_cores=1,
    num_executors=2,
    dag=dag
)

data_analysis = SparkSubmitOperator(
    task_id='data_analysis',
    application='/opt/airflow/scripts/data_analysis.py',
    name='data_analysis',
    application_args=['/opt/airflow/data/standard_raw/'],
    executor_memory='2g',
    executor_cores=1,
    num_executors=2,
    dag=dag
)

read_files >> data_analysis
