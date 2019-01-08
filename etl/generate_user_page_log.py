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
# Tomorrow (for partitioning)
TDY = """{{ (execution_date + macros.timedelta(days = 1)).strftime("%Y") }}"""
TDM = """{{ (execution_date + macros.timedelta(days = 1)).strftime("%m") }}"""
TDD = """{{ (execution_date + macros.timedelta(days = 1)).strftime("%d") }}"""

# SQL Templates
insert_sql = """
INSERT INTO staging.user_page_log_%(lhdy)s_%(lhdm)s_%(lhdd)s (time_received, uri,
    referrer, user_agent, has_wallet, ip, session_id, user_id, user_id_uuid,
    ga_ga, ga_gid)
    SELECT time_received, uri, referrer, user_agent, has_wallet, ip, session_id,
    user_id, user_id_uuid, ga_ga, ga_gid
    FROM staging.raw_s3_logs
    WHERE (not uri ~ '^/notification/')
        and time_received >= '%(lhdy)s-%(lhdm)s-%(lhdd)s %(lhdh)s:00:00'
        and time_received < '%(dy)s-%(dm)s-%(dd)s %(dh)s:00:00'
"""

create_sql = """
CREATE TABLE IF NOT EXISTS staging.user_page_log_%(lhdy)s_%(lhdm)s_%(lhdd)s
    (time_received timestamp, uri text, referrer text, user_agent text,
    has_wallet boolean, ip inet, session_id text, user_id int, user_id_uuid uuid,
    ga_ga text, ga_gid text, row_added timestamp default now())
"""

attach_part_sql = """
ALTER TABLE staging.user_page_log ATTACH PARTITION staging.user_page_log_%(lhdy)s_%(lhdm)s_%(lhdd)s
FOR VALUES FROM ('%(lhdy)s-%(lhdm)s-%(lhdd)s 00:00:00') TO ('%(tdy)s-%(tdm)s-%(tdd)s 00:00:00' )
"""

sense_sql = """
SELECT * from staging.raw_s3_logs
WHERE
    time_received >= '%(lhdy)s-%(lhdm)s-%(lhdd)s %(lhdh)s:00:00'
    and time_received < '%(dy)s-%(dm)s-%(dd)s %(dh)s:00:00'
"""

default_args = {
    'owner': 'michael.misiewicz',
    'depends_on_past': True,
    'start_date': datetime(2019, 1, 1, 0),
    'email': ['michael.misiewicz@consensys.net'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('generate_user_page_log', default_args=default_args,
          schedule_interval='@hourly')

sensor = SqlSensor(
    task_id="sensor",
    conn_id='postgres_data_warehouse',
    sql=sense_sql % {'dy':DY, 'dm':DM, 'dd':DD, 'dh':DH, 'lhdy':LHDY,
                     'lhdm':LHDM, 'lhdd':LHDD, 'lhdh':LHDH},
    dag=dag
)

create_partitions = PostgresOperator(
    task_id="create_table",
    postgres_conn_id='postgres_data_warehouse',
    sql=create_sql % {'dy':DY, 'dm':DM, 'dd':DD, 'dh':DH, 'lhdy':LHDY,
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

def attach_partition(**context):
    pg = PostgresHook(postgres_conn_id='postgres_data_warehouse')
    conn = pg.get_conn()
    cur = conn.cursor()
    try:
        print(attach_part_sql % context['templates_dict'])
        cur.execute(attach_part_sql % context['templates_dict'])
        conn.commit()
        conn.close()
    except (psycopg2.ProgrammingError):
        print("Warning: ignoring exception creating partition")
        pass

attach_partitions = PythonOperator(
    task_id="attach_partitions",
    python_callable=attach_partition,
    provide_context=True,
    templates_dict={'lhdy':LHDY, 'lhdm':LHDM, 'lhdd':LHDD,
                    'tdy':TDY, 'tdm':TDM, 'tdd':TDD},
    dag=dag
)

create_partitions.set_upstream(sensor)
move_rows_to_partitions.set_upstream(create_partitions)
attach_partitions.set_upstream(move_rows_to_partitions)
