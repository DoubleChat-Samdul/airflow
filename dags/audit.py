from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from kafka import KafkaProducer
from json import dumps
import pandas as pd
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 24),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_for_audit',
    default_args=default_args,
    description='fetch data from kafka hourly for audit',
    schedule='@hourly',
    catchup=False,
)


def process_data():
    output_path = os.getenv('AUDIT_PATH')

    df = pd.read_parquet(output_path)

    df = df[df["sender"] != "[INFO]"]
    df = df[df["end"] != True]
    df["message"] = df["message"].str.replace("\n", " ")
    
    if df["timestamp"].dtype == 'object': 
        df["timestamp"] = pd.to_datetime(df["timestamp"], format="%Y-%m-%dT%H:%M:%S.%f")

    df["date"] = df["timestamp"].dt.strftime('%Y-%m-%d')
    df.to_parquet(output_path, partition_cols=['date'], index=False)    

start_task = EmptyOperator(
    task_id='start',
    dag=dag,
)

end_task = EmptyOperator(
    task_id='end',
    dag=dag,
)

fetch_task = BashOperator(
    task_id='fetch_data',
    bash_command="""
        $SPARK_HOME/bin/spark-submit $AUDIT_MODULE
    """
)

utilize_task = PythonOperator(
    task_id='process_utilize',
    python_callable=process_data,
    dag=dag,
)

start_task >> fetch_task >> utilize_task >> end_task
