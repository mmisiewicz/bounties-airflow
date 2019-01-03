import os

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import S3KeySensor
from airflow.hooks.S3_hook import S3Hook

default_args = {
    'owner': 'michael.misiewicz',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 1),
    'email': ['michael.misiewicz@consensys.net'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('s3_dag_test', default_args=default_args, schedule_interval= '@hourly')

filepath = """{{ execution_date.strftime("%Y") }}/{{ execution_date.strftime("%m") }}/{{ execution_date.strftime("%d") }}/{{ execution_date.strftime("%H") }}"""

t1 = BashOperator(
    task_id='bash_test',
    bash_command='echo "hello, it should work" > s3_conn_test.txt',
    dag=dag)

sensor = S3KeySensor(
    task_id='check_s3_for_file_in_s3',
    bucket_key=filepath + "/*.log",
    wildcard_match=True,
    bucket_name='bountiesapilog',
    s3_conn_id='bounties_s3',
    timeout=18*60*60,
    poke_interval=120,
    dag=dag)

t1.set_upstream(sensor)
