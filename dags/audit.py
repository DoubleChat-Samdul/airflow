from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from kafka import KafkaProducer
from json import dumps

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
    schedule_interval='@hourly',
    catchup=False,
)

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
        $SPARK_HOME/bin/spark-submit /home/kyuseok00/teamproj/chat/py/audit.py
    """
)

start_task >> fetch_task >> end_task

