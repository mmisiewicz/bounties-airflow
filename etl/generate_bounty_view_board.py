from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import psycopg2
from airflow.sensors.sql_sensor import SqlSensor
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

# Current hour macros
DY = """{{ execution_date.strftime("%Y") }}"""
DM = """{{ execution_date.strftime("%m") }}"""
DD = """{{ execution_date.strftime("%d") }}"""
DH = """{{ execution_date.strftime("%H") }}"""
# Last hour's partition
LHDY = """{{ prev_execution_date.strftime("%Y") }}"""
LHDM = """{{ prev_execution_date.strftime("%m") }}"""
LHDD = """{{ prev_execution_date.strftime("%d") }}"""
LHDH = """{{ prev_execution_date.strftime("%H") }}"""

# SQL Templates
insert_sql = """
INSERT INTO public.log_bounty_view (time_received, bounty_id, user_agent,
    has_wallet, ip, session_id, user_id, user_id_uuid, ga_ga, ga_gid)
    SELECT time_received, substring(referrer from '/bounty/([0-9]+)')::int as bounty_id,
    user_agent, has_wallet, ip, session_id, user_id, user_id_uuid, ga_ga, ga_gid
    FROM staging.user_page_log
    WHERE uri = '/analytics/ping/' and referrer ~ '/bounty/[0-9]+'
        and time_received >= '%(lhdy)s-%(lhdm)s-%(lhdd)s %(lhdh)s:00:00'
        and time_received < '%(dy)s-%(dm)s-%(dd)s %(dh)s:00:00'
"""

sense_sql = """
SELECT * from staging.user_page_log
WHERE
    time_received >= '%(lhdy)s-%(lhdm)s-%(lhdd)s %(lhdh)s:00:00'
"""

delete_sql = """DELETE FROM public.log_bounty_view
WHERE time_received >= '%(lhdy)s-%(lhdm)s-%(lhdd)s %(lhdh)s:00:00'
      and time_received < '%(dy)s-%(dm)s-%(dd)s %(dh)s:00:00'
"""

default_args = {
    'owner': 'michael.misiewicz',
    'depends_on_past': True,
    'start_date': datetime(2019, 1, 21, 0),
    'email': ['michael.misiewicz@consensys.net'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('generate_bounty_view_board', default_args=default_args,
          schedule_interval='@hourly')

sensor = SqlSensor(
    task_id="sensor",
    conn_id='postgres_data_warehouse',
    sql=sense_sql % {'dy':DY, 'dm':DM, 'dd':DD, 'dh':DH, 'lhdy':LHDY,
                     'lhdm':LHDM, 'lhdd':LHDD, 'lhdh':LHDH},
    dag=dag
)

move_rows_to_partitions = PostgresOperator(
    task_id="move_rows_to_partitions",
    postgres_conn_id='postgres_data_warehouse',
    sql=insert_sql % {'dy':DY, 'dm':DM, 'dd':DD, 'dh':DH, 'lhdy':LHDY,
                      'lhdm':LHDM, 'lhdd':LHDD, 'lhdh':LHDH},
    dag=dag
)

clear_partitions = PostgresOperator(
    task_id="clear_partitions",
    postgres_conn_id='postgres_data_warehouse',
    sql=delete_sql % {'dy':DY, 'dm':DM, 'dd':DD, 'dh':DH, 'lhdy':LHDY,
                      'lhdm':LHDM, 'lhdd':LHDD, 'lhdh':LHDH},
    dag=dag
)

clear_partitions.set_upstream(sensor)
move_rows_to_partitions.set_upstream(clear_partitions)
