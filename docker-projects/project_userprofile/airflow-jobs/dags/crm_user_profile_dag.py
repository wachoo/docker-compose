from __future__ import print_function
from datetime import timedelta
import datetime as dt
import airflow
from airflow.models import Variable
from os import listdir, getenv
from os.path import isfile, join
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.check_operator import CheckOperator, ValueCheckOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.hive_hooks import HiveCliHook
from wachoo.hooks.es_hook import ESHook
import logging
import pendulum

local_tz = pendulum.timezone("Asia/Shanghai")

logging.basicConfig()

HIVE_CONN_ID = 'hive_cli_default'
HIVE_SERVER_CONN_ID = 'hiveserver2_default'
es_conn = ESHook.get_connection('es_default')
es_host, es_port = es_conn.host, es_conn.port
print(f'es_host:{es_host},es_port:{es_port}')


default_args = {
    'owner': 'feng',
    'start_date': dt.datetime(2019, 10, 1, tzinfo=local_tz),
    'email': ['someone@yeah.net'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

default_sql_path = getenv('HOME') + '/sql'
print('default_sql_path: {}'.format(default_sql_path))
tmpl_search_path = Variable.get('sql_path', default_var=default_sql_path)

PARAMS = {
}

def collect_tag_id_and_datatype_from_sql_file(file_name):
    # global tag_and_datatype_dict

    with open(dag.template_searchpath[0] + '/' + file_name) as opened_sql_file:
        tag_id_found = False
        tag_id_str, tag_datatype = None, None
        for line in opened_sql_file:
            if tag_id_found:
                datatype_raw = line.split(',')[1]
                tag_datatype = datatype_raw.replace("'", "").replace(' ', '').replace('\n', '')

                tag_and_datatype_dict[tag_id_str] = tag_datatype
                tag_id_found = False
                continue

            if line.startswith('partition (tag_id='):
                tag_id_raw = line.split('=')[1]
                tag_id_str = tag_id_raw.replace("'", "").replace(')', '').replace('\n', '')
                tag_id_found = True


# 主流程
dag = airflow.DAG(
    'user_tag_etl_dag',
    schedule_interval="10 0 * * *",
    dagrun_timeout=timedelta(minutes=60),
    template_searchpath=tmpl_search_path + '/crm',
    default_args=default_args,
    catchup=False,
    max_active_runs=1)


only_files = [f for f in listdir(tmpl_search_path + '/crm') if isfile(join(tmpl_search_path + '/crm', f))]
# print(only_files)

indexed_files = [(int(fn.split('.')[0]), fn) for fn in only_files]
sorted_files = sorted(indexed_files, key=lambda tup: tup[0])

# print(sorted_files)

# 储存tag_id和datatype的字典
tag_and_datatype_dict = {}

# 创建顺序执行的DAG工作流

# 检查源数据是否符合要求

check_account_table = CheckOperator(
    task_id='check_account_table',
    conn_id=HIVE_SERVER_CONN_ID,
    sql="select count(*) from crm_accounts --where createdat >= '{{ ds }}'",
    dag=dag
)

check_order_table = CheckOperator(
    task_id='check_order_table',
    conn_id=HIVE_SERVER_CONN_ID,
    sql="select count(*) from crm_orders --where createdat >= '{{ ds }}'",
    dag=dag
)

# 所有检查任务完成
source_check_completed = DummyOperator(
    task_id='source_check_completed',
    dag=dag
)

# 设置源检查任务依赖
check_account_table >> source_check_completed
check_order_table >> source_check_completed

tail_etl_task = source_check_completed

for tp in sorted_files:
    file_name = tp[1]
    etl_name = tp[1].split('.')[1]
    etl_task = HiveOperator(
        task_id='user_tag_etl__' + etl_name,
        hql=file_name,
        hive_cli_conn_id=HIVE_CONN_ID,
        dag=dag
    )

    if tail_etl_task is not None:
        etl_task.set_upstream(tail_etl_task)
        tail_etl_task = etl_task
    else:
        tail_etl_task = etl_task

    collect_tag_id_and_datatype_from_sql_file(file_name)

print('tag_and_datatype_dict: {}'.format(tag_and_datatype_dict))

# 窄表转宽表
user_tag_to_wide_sql = '''
drop table if exists user_tag_wide;
create table user_tag_wide
as
select
user_id
'''

tag_datatype_mapping = {
    'date': 'tag_date_value',
    'string': 'tag_str_value',
    'decimal': 'tag_decimal_value'
}

for tag_id, tag_datatype in tag_and_datatype_dict.items():
    tag_sql_frag = ",max(case when tag_id = '{tag_id}' then {value_field} end) as {tag_id}\n".format(
        tag_id=tag_id,
        value_field=tag_datatype_mapping[tag_datatype])
    user_tag_to_wide_sql = user_tag_to_wide_sql + tag_sql_frag

user_tag_to_wide_sql = user_tag_to_wide_sql + '''from user_tag_storage
where tag_id in (
'{tag_ids_str}'
)
group by user_id;
'''.format(tag_ids_str="','".join(tag_and_datatype_dict.keys()))

#print('user_tag_to_wide_sql: {}'.format(user_tag_to_wide_sql))

# 窄表转宽表的Task
user_tag_wide_table_task = HiveOperator(
    task_id='user_tag_wide_table',
    hql=user_tag_to_wide_sql,
    hive_cli_conn_id=HIVE_CONN_ID,
    dag=dag
)

user_tag_wide_table_task.set_upstream(tail_etl_task)

# ETL结果检查任务
check_etl_result = ValueCheckOperator(
    task_id='check_etl_result',
    sql='''
    select count(register_date)/count(0) c1, count(case when account_name != '' then 1 end)/count(0) c2
    from user_tag_wide
    ''',
    pass_value=0.9,
    tolerance=0.2,
    conn_id=HIVE_SERVER_CONN_ID,
    dag=dag
)

# 检查结果记录数是否和源表一致
check_result_row_count = CheckOperator(
    task_id='check_result_row_count',
    conn_id=HIVE_SERVER_CONN_ID,
    sql="""
    select if(A.cnt - B.cnt = 0, 1, 0)
    from (select 1 as foo, count(*) cnt from user_tag_wide) A 
    left join (select 1 as foo, count(*) cnt from user_tag_storage where tag_id='account_name') B
           on A.foo=B.foo
    """,
    dag=dag
)

create_es_export_table = HiveOperator(
    task_id='create_es_export_table',
    hql='''
add jar hdfs:///tmp/elasticsearch-hadoop-6.4.2.jar;
drop table es_crm_user_profile;
CREATE EXTERNAL TABLE es_crm_user_profile (
user_id bigint,
register_date bigint,
invite_code STRING,
account_name string,
birthday bigint,
gender string,
family_income float,
marital_status string,
has_fill_completed  string,
work_company        string,
work_salary         float,
work_entry_date     bigint,
card_bind_date      bigint,
bank_account_name   string,
card_bank_code      string,
has_loan_appl       string,
is_first_loan_appl  string,
is_loan_disbursed   string,
loan_purpose        string,
is_loan_approved    string
)
STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES(  'es.mapping.id' = 'user_id',
                'es.resource' = 'user_profile_demo{{{{ds}}}}/_doc',
                'es.nodes' = 'http://{es_host}:{es_port}',
                'es.nodes.wan.only' = 'true',
                'es.index.auto.create' = 'true');
'''.format(es_host=es_host, es_port=es_port),
    hiveconfs={'stat_date':'{{ds}}'},
    hive_cli_conn_id=HIVE_CONN_ID,
    dag=dag
)

export_data_to_es = HiveOperator(
    task_id='export_data_to_es',
    hql='''
add jar hdfs:///tmp/elasticsearch-hadoop-6.4.2.jar;
insert overwrite table es_crm_user_profile
select 
user_id, 
unix_timestamp(register_date,'yyyy-MM-dd')*1000 as register_date, 
invite_code, account_name,
unix_timestamp(birthday,'yyyy-MM-dd')*1000 as birthday, 
gender, family_income, marital_status,
has_fill_completed, work_company, work_salary,
unix_timestamp(work_entry_date,'yyyy-MM-dd')*1000 as work_entry_date, 
unix_timestamp(card_bind_date,'yyyy-MM-dd')*1000 as card_bind_date, 
bank_account_name,
card_bank_code, has_loan_appl, is_first_loan_appl,
is_loan_disbursed, loan_purpose, is_loan_approved

 from user_tag_wide
limit 1000;
''',
    hive_cli_conn_id=HIVE_CONN_ID,
    dag=dag
)

backup_data_to_his = HiveOperator(
    task_id='backup_data_to_his',
    hql='''
CREATE EXTERNAL TABLE if not exists crm_user_profile_his (
user_id bigint,
register_date bigint,
invite_code STRING,
account_name string,
birthday bigint,
gender string,
family_income float,
marital_status string,
has_fill_completed  string,
work_company        string,
work_salary         float,
work_entry_date     bigint,
card_bind_date      bigint,
bank_account_name   string,
card_bank_code      string,
has_loan_appl       string,
is_first_loan_appl  string,
is_loan_disbursed   string,
loan_purpose        string,
is_loan_approved    string
)
partitioned by (stat_date string)
stored as parquet;

insert overwrite table crm_user_profile_his
partition(stat_date='${hiveconf:stat_date}')
select * from user_tag_wide;
''',
    hive_cli_conn_id=HIVE_CONN_ID,
    hiveconfs={'stat_date':'{{ds}}'},
    dag=dag
)

user_tag_wide_table_task >> check_etl_result >> check_result_row_count >> \
create_es_export_table >> export_data_to_es >> backup_data_to_his

if __name__ == "__main__":
    dag.cli()
