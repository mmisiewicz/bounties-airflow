
import os
import re
import ast
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator

REGEX = re.compile("\[pid:\s(?P<pid>\d*)\|app:\s-\|req:\s-\/-] (?P<ip>\d*\.\d*\.\d*\.\d*) \(-\)\s\{(?P<num_request_variables>\d*)\svars\sin\s(?P<packet_size>\d*)\sbytes}\s\[(?P<time_received>.*)]\s(?P<http_method>\w*)\s(?P<uri>\/.*)\s=(?:\\u003e|>)\sgenerated\s(?P<response_size>\d*)\sbytes\sin\s(?P<response_time>\d*)\smsecs\s\(HTTP\/(?:\d*\.\d*)\s(?P<http_status>\d*)\)\s(?P<num_headers>\d*) headers\sin\s(?P<header_size>\d*)\sbytes\s\((?P<switches>\d*)\sswitches\son\score\s(?P<core>\d*)\)\sreferrer:\s(?P<referrer>[^\s]*)\sUA\s(?P<user_agent>.*)\shas\swallet:\s(?P<has_wallet>.*)\scookies:\s(?P<cookies>.*)\\n")

sql_create_partition_command = """
CREATE TABLE IF NOT EXISTS staging.raw_s3_logs_%(y)s_%(m)s_%(d)s
(pid int, ip inet, num_request_variables int, packet_size int, time_received timestamp,
http_method varchar(10), uri text, response_size int, http_status int, num_headers int,
header_size int, switches int, core int, referrer text, user_agent text,
has_wallet boolean, session_id text, user_id int, user_id_uuid uuid, ga_ga text, ga_gid text,
row_added timestamp default now())
;
"""

attach_part_sql = """
    ALTER TABLE staging.raw_s3_logs ATTACH PARTITION staging.raw_s3_logs_%(dy)s_%(dm)s_%(dd)s
    FOR VALUES FROM ('%(dy)s-%(dm)s-%(dd)s 00:00:00') TO ('%(tdy)s-%(tdm)s-%(tdd)s 00:00:00' )
"""

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

sql_truncate_table_command = """ DELETE FROM staging.raw_s3_logs_%(y)s_%(m)s_%(d)s
WHERE date_trunc('hour',time_received) = '%(y)s-%(m)s-%(d)s %(h)s:00:00'"""

default_args = {
    'owner': 'michael.misiewicz',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 21, 0),
    'email': ['michael.misiewicz@consensys.net'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('s3_log_etl', default_args=default_args, schedule_interval= '@hourly')
# previous hour
DY = """{{ prev_execution_date.strftime("%Y") }}"""
DM = """{{ prev_execution_date.strftime("%m") }}"""
DD = """{{ prev_execution_date.strftime("%d") }}"""
DH = """{{ prev_execution_date.strftime("%H") }}"""
# yesterday's partition
YDY = """{{ (prev_execution_date - macros.timedelta(days = 1)).strftime("%Y") }}"""
YDM = """{{ (prev_execution_date - macros.timedelta(days = 1)).strftime("%m") }}"""
YDD = """{{ (prev_execution_date - macros.timedelta(days = 1)).strftime("%d") }}"""
# tomorrow
TDY = """{{ (prev_execution_date + macros.timedelta(days = 1)).strftime("%Y") }}"""
TDM = """{{ (prev_execution_date + macros.timedelta(days = 1)).strftime("%m") }}"""
TDD = """{{ (prev_execution_date + macros.timedelta(days = 1)).strftime("%d") }}"""


FILEPATH = """%s/%s/%s/%s""" % (DY, DM, DD, DH)

def process_cookie_list(cookies_string):
    cookie_dict_clean = {}
    cookies_list = cookies_string.split(';')
    for cookie in cookies_list:
        cookiesplit = cookie.split('=')
        if len(cookiesplit) > 1:
            k = cookiesplit[0].strip()
            v = cookiesplit[1].strip()
            if k == 'user_id':
                v = int(v)
            cookie_dict_clean[k] = v
    return cookie_dict_clean

def parse_line(line):
    # always use protection!
    line_dict = ast.literal_eval(line)
    if line_dict['log'].startswith('WARNING:django.request:Not Found:'):
        return None
    if line_dict['log'].startswith('Not Found:'):
        return None

    nougat_center = REGEX.match(line_dict['log']).groupdict()
    # clean cookies function casts data to correct type
    clean_cookies = process_cookie_list(nougat_center['cookies'])
    # coerce data to needed types (if any fail, the entire line will be logged as bad)
    has_wallet_bool = (nougat_center['has_wallet'] == 'true')
    event_timestamp = datetime.strptime(nougat_center['time_received'], "%c")
    nougat_center['time_received'] = event_timestamp
    nougat_center['has_wallet'] = has_wallet_bool
    nougat_center['pid'] = int(nougat_center['pid'])
    nougat_center['num_request_variables'] = int(nougat_center['num_request_variables'])
    nougat_center['packet_size'] = int(nougat_center['packet_size'])
    nougat_center['response_size'] = int(nougat_center['response_size'])
    nougat_center['response_time'] = int(nougat_center['response_time'])
    nougat_center['http_status'] = int(nougat_center['http_status'])
    nougat_center['num_headers'] = int(nougat_center['num_headers'])
    nougat_center['header_size'] = int(nougat_center['header_size'])
    nougat_center['switches'] = int(nougat_center['switches'])
    nougat_center['core'] = int(nougat_center['core'])
    nougat_center['session_id'] = clean_cookies.get('session_id')
    nougat_center['user_id'] = clean_cookies.get('user_id')
    nougat_center['user_id_uuid'] = clean_cookies.get('uuid')
    nougat_center['ga_ga'] = clean_cookies.get('_ga')
    nougat_center['ga_gid'] = clean_cookies.get('_gid')
    return nougat_center

def process_log_files(**context):
    s3 = S3Hook() # aws_conn_id="bounties_s3"
    filepath = context['templates_dict']['filepath']
    dy = context['templates_dict']['dy']
    dm = context['templates_dict']['dm']
    dd = context['templates_dict']['dd']
    dh = context['templates_dict']['dh']
    files = s3.list_keys("bountiesapilog", prefix=filepath)
    files_processed = []
    log_data = []
    bad_regex_lines = []
    for file in files:
        print("Processing file: " + file)
        file_line_count = 0
        file_bad_lines = 0
        file_regex_failed = 0
        data_body = s3.get_key(bucket_name = "bountiesapilog", key=file).get()
        lines = data_body["Body"].read().decode("utf-8").split("\n")
        for line in lines:
            file_line_count += 1
            try:
                nougat_center = parse_line(line)
                if nougat_center != None:
                    log_data.append(nougat_center)
            except (KeyError, ValueError, TypeError, SyntaxError):
                print("Malformed line: " + line[0:500])
                file_bad_lines += 1
            except (AttributeError):
                print("Regex error in file:line : %s:%s " % (file:file_line_count))
                bad_regex_lines.append({'filename': file,
                                        'line_number': file_line_count,
                                        'bad_line': line})
                file_regex_failed += 1
            except:
                print("Unhandled exception.")
                print("The offending line is: " + line)
                raise
        files_processed.append({'filename':file, 'file_line_count':file_line_count,
            'file_bad_lines':file_bad_lines, 'file_regex_failed':file_regex_failed})
    # now all data has been decoded, and we have a list of how this day's files
    # fared. Next: insert into the db!
    pg = PostgresHook(postgres_conn_id='postgres_data_warehouse')
    conn = pg.get_conn()
    cur = conn.cursor()
    stage_table_name = "staging.raw_s3_logs_{y}_{m}_{d}".format(**{'y':dy, 'm':dm, 'd':dd})
    cur.executemany("""INSERT INTO staging.files_processed (filename, file_line_count, file_bad_lines, file_regex_failed)
        VALUES (%(filename)s, %(file_line_count)s, %(file_bad_lines)s, %(file_regex_failed)s)""", files_processed)
    cur.executemany("""INSERT INTO staging.raw_regex_failure_lines (filename, line_number, bad_line)
        VALUES (%(filename)s, %(line_number)s, %(bad_line)s)""", bad_regex_lines)
    conn.commit()
    cur.executemany("INSERT INTO " + stage_table_name +
        """ (pid, ip, num_request_variables, packet_size, time_received,
            http_method, uri, response_size, http_status, num_headers,
            header_size, switches, core, referrer, user_agent, has_wallet,
            session_id, user_id, user_id_uuid, ga_ga, ga_gid)
        VALUES (%(pid)s, %(ip)s, %(num_request_variables)s,
            %(packet_size)s, %(time_received)s, %(http_method)s,
            %(uri)s, %(response_size)s, %(http_status)s,
            %(num_headers)s, %(header_size)s, %(switches)s,
            %(core)s, %(referrer)s, %(user_agent)s,
            %(has_wallet)s, %(session_id)s, %(user_id)s,
            %(user_id_uuid)s, %(ga_ga)s, %(ga_gid)s)""",
        log_data)
    conn.commit()
    conn.close()

attach_partitions = PythonOperator(
    task_id="attach_partitions",
    python_callable=attach_partition,
    provide_context=True,
    templates_dict={'dy':DY, 'dm':DM, 'dd':DD,
                    'tdy':TDY, 'tdm':TDM, 'tdd':TDD},
    dag=dag
)

load_logs_to_postgres = PythonOperator(
    task_id='parse_logs_and_load_to_staging',
    python_callable=process_log_files,
    provide_context=True,
    templates_dict={'filepath':FILEPATH, 'dy':DY, 'dm':DM, 'dd':DD, 'dh':DH},
    dag=dag)

sensor = S3KeySensor(
    task_id='check_s3_for_file_in_s3',
    bucket_key=FILEPATH +"/*.log",
    wildcard_match=True,
    bucket_name='bountiesapilog',
    s3_conn_id='bounties_s3',
    timeout=18*60*60,
    poke_interval=120,
    dag=dag)

clear_partitions = PostgresOperator(
    task_id='clear_partitions',
    postgres_conn_id='postgres_data_warehouse',
    sql=sql_truncate_table_command % {'y':DY, 'm':DM, 'd':DD, 'h':DH},
    dag=dag
)

create_partitions = PostgresOperator(
    task_id="create_table",
    postgres_conn_id='postgres_data_warehouse',
    sql=sql_create_partition_command % {'y':DY, 'm':DM, 'd':DD},
    dag=dag
)


move_rows_to_partitions = PostgresOperator(
    task_id="move_rows_to_partitions",
    postgres_conn_id='postgres_data_warehouse',
    sql="""
    INSERT INTO staging.raw_s3_logs_%(yy)s_%(ym)s_%(yd)s (pid, ip, num_request_variables, packet_size, time_received,
        http_method, uri, response_size, http_status, num_headers,
        header_size, switches, core, referrer, user_agent, has_wallet,
        session_id, user_id, user_id_uuid, ga_ga, ga_gid)
        SELECT pid, ip, num_request_variables, packet_size, time_received,
            http_method, uri, response_size, http_status, num_headers,
            header_size, switches, core, referrer, user_agent, has_wallet,
            session_id, user_id, user_id_uuid, ga_ga, ga_gid
        FROM staging.raw_s3_logs_%(y)s_%(m)s_%(d)s
        WHERE time_received < '%(y)s-%(m)s-%(d)s 00:00:00';
        DELETE FROM staging.raw_s3_logs_%(y)s_%(m)s_%(d)s
        WHERE time_received < '%(y)s-%(m)s-%(d)s 00:00:00';
        """ % {'y':DY, 'm':DM, 'd':DD, 'yy':YDY, 'ym':YDM, 'yd':YDD},
    dag=dag
)

# attach_partitions = PostgresOperator(
#     task_id = "attach_partitions",
#     postgres_conn_id = 'postgres_data_warehouse',
#     sql =  % {'y':DY, 'm':DM, 'd':DD, 'tdy':TDY, 'tdm':TDM, 'tdd':TDD},
#     dag=dag
# )

clear_partitions.set_upstream(create_partitions)
create_partitions.set_upstream(sensor)
load_logs_to_postgres.set_upstream(clear_partitions)
move_rows_to_partitions.set_upstream(load_logs_to_postgres)
attach_partitions.set_upstream(move_rows_to_partitions)
