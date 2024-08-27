from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from kafka import KafkaProducer
from json import dumps
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
'''
home_dir = os.path.expanduser("~")
audit_path = os.path.join(home_dir,'teamproj/data/messages_audit')
audit_module = os.path.join(home_dir,'teamproj/chat/src/audit/audit.py')
offset_path = os.path.join(home_dir,'teamproj')
'''

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
        export AUDIT_PATH=$AUDIT_PATH
        export OFFSET_PATH=$OFFSET_PATH
        export AUDIT_MODULE=$AUDIT_MODULE
        $SPARK_HOME/bin/spark-submit $AUDIT_MODULE
    """
)

start_task >> fetch_task >> end_task
