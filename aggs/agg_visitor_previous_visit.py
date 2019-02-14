from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import psycopg2
from airflow.sensors.sql_sensor import SqlSensor
from airflow.operators.postgres_operator import PostgresOperator


# Yesterday
YDY = """{{ prev_execution_date.strftime("%Y") }}"""
YDM = """{{ prev_execution_date.strftime("%m") }}"""
YDD = """{{ prev_execution_date.strftime("%d") }}"""
# Day before Yesterday ("yesterday yesterday")
YYDY = """{{ (execution_date - macros.timedelta(days = 2)).strftime("%Y") }}"""
YYDM = """{{ (execution_date - macros.timedelta(days = 2)).strftime("%m") }}"""
YYDD = """{{ (execution_date - macros.timedelta(days = 2)).strftime("%d") }}"""

MACROS = {'YDY': YDY, 'YDM': YDM, 'YDD': YDD, 'YYDY': YYDY, 'YYDM': YYDM, 'YYDD': YYDD}
# SQL Command to check for a closed day
sense_sql = """
SELECT * from staging.user_page_log
WHERE
    time_received >= '%(YDY)s-%(YDM)s-%(YDD)s 23:00:00'
"""

default_args = {
    'owner': 'michael.misiewicz',
    'depends_on_past': True,
    'start_date': datetime(2019, 1, 10),
    'email': ['michael.misiewicz@consensys.net', 'matt.garnett@consensys.net'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('agg_visitor_previous_visit', default_args=default_args, schedule_interval= '@daily')

insert_cmd = """INSERT INTO agg_visitor_previous_visit (user_id_uuid, user_id, ymd, has_wallet, pageviews, last_visit)
with daily_pageviews as (select user_id_uuid, user_id, max(has_wallet::int)::boolean as has_wallet,
                         count(*) as pageviews
                         from staging.user_page_log where date(time_received) = '%(YDY)s-%(YDM)s-%(YDD)s'
                         group by user_id_uuid, user_id
),
all_past_pageviews as (select user_id_uuid, user_id, max(date(time_received)) as ymd from staging.user_page_log
                       where date(time_received) <= '%(YYDY)s-%(YYDM)s-%(YYDD)s'
                       group by user_id_uuid, user_id)
select daily_pageviews.user_id_uuid, daily_pageviews.user_id, '%(YDY)s-%(YDM)s-%(YDD)s'::date as ymd, has_wallet, pageviews,
coalesce(all_past_pageviews.ymd, '-Infinity'::date) as last_visit
from daily_pageviews left join all_past_pageviews on daily_pageviews.user_id_uuid = all_past_pageviews.user_id_uuid
"""

delete_cmd = "DELETE FROM agg_visitor_previous_visit WHERE ymd = '%(YDY)s-%(YDM)s-%(YDD)s' "

sensor = SqlSensor(
    task_id="sensor",
    conn_id='postgres_data_warehouse',
    sql=sense_sql % MACROS,
    poke_interval= 15 * 60, # minutes
    timeout = 60 * 60 * 13, # 13h, S3 dag has 12h timeout
    dag=dag
)

delete = PostgresOperator(
    task_id="delete",
    postgres_conn_id='postgres_data_warehouse',
    sql=delete_cmd % MACROS,
    dag=dag
)

insert = PostgresOperator(
    task_id="insert",
    postgres_conn_id='postgres_data_warehouse',
    sql=insert_cmd % MACROS,
    dag=dag
)

delete.set_upstream(sensor)
insert.set_upstream(delete)
