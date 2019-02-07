from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import psycopg2
from airflow.sensors.sql_sensor import SqlSensor
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

default_args = {
    'owner': 'michael.misiewicz',
    'depends_on_past': False,
    'start_date': datetime(2019, 2, 6),
    'email': ['michael.misiewicz@consensys.net'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('agg_bounty_snapshot', default_args=default_args, schedule_interval= '@daily')

insert_cmd = """INSERT INTO agg_bounty_snapshot (bounty_id, title, description, bounty_stage,
            usd_price, user_id, deadline, created, ymd)
            SELECT bounty_id, title, description, "bountyStage" as bounty_stage,
            usd_price, user_id, deadline, created, '{{ ds }}' as ymd from bounties.std_bounties_bounty """
delete_cmd = "DELETE FROM agg_bounty_snapshot WHERE ymd = '{{ ds }}' "

delete = PostgresOperator(
    task_id="delete",
    postgres_conn_id='postgres_data_warehouse',
    sql=delete_cmd,
    dag=dag
)

insert = PostgresOperator(
    task_id="insert",
    postgres_conn_id='postgres_data_warehouse',
    sql=insert_cmd,
    dag=dag
)

insert.set_upstream(delete)
