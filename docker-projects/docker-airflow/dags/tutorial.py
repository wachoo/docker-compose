

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.dummy_operator import DummyOperator
from wachoo.operators.mysql2mysql_operators import MySQLToMySQLOperator
from datetime import datetime, timedelta
from airflow.models import Variable

import pendulum
import requests
from pprint import pprint
import os

# 配置时区
local_tz = pendulum.timezone("Asia/Shanghai")

default_args = {
    "owner": "magicwind",
    # 不依赖之前的任务是否成功
    "depends_on_past": False,
    # 开始时间
    "start_date": datetime(2020, 6, 8, tzinfo=local_tz),
    # 重试一次
    "retries": 1,
    # 重试间隔
    "retry_delay": timedelta(minutes=5)
}

app_id = Variable.get('exchange_rate_app_id')
if not app_id:
    print('exchange_rate_app_id is not configured')
    exit(0)

dag = DAG(
    "tutorial",
    default_args=default_args,
    # DAG RUN 超时时间
    dagrun_timeout=timedelta(minutes=10),
    # 定时调度配置
    schedule_interval="10 10 * * *",
    # 不要自动创建历史的DAG RUN
    catchup=False,
    # 每个DAG同时只能有一个DAG RUN在执行
    max_active_runs=1
)

t1 = BashOperator(task_id="print_date", bash_command="date", dag=dag)

t2 = BashOperator(task_id="sleep", bash_command="sleep 5", dag=dag)

templated_command = """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
        echo "{{ params.my_param }}"
    {% endfor %}
"""

t3 = BashOperator(
    task_id="templated",
    bash_command=templated_command,
    params={"my_param": "Parameter I passed in"},
    dag=dag,
)

const_filepath = '/tmp/exchange_rate.csv'


def get_exchange_rate(**context):
    print('********Start*********')
    tmp_file_path = context['templates_dict']['tmp_file_path']
    target_date = context['templates_dict']['target_date']
    in_app_id =  context['templates_dict']['app_id']
    print(f'tmp_file_path:{tmp_file_path}, target_date:{target_date}, in_app_id:{len(in_app_id)} chars')

    #5c2dba0f887544a4b196eeaa8a3052f4
    resp = requests.get(f'https://openexchangerates.org/api/historical/{target_date}.json?app_id={in_app_id}')
    result = resp.json()
    pprint(result)

    base_currency = result['base']
    data_timestamp = result['timestamp']
    date_date = datetime.utcfromtimestamp(data_timestamp).date().isoformat()
    rates = result['rates']

    rows = ['base,date,currency,rate']
    for key in rates:
        rate_in_dec = '{0:f}'.format(rates[key])
        rows.append(f'{base_currency},{date_date},{key},{rate_in_dec}\n')

    for r in rows:
        print(r[:-1])

    filepath = '/tmp/exchange_rate.csv'

    print(f'write result to file: {filepath}')
    if os.path.exists(filepath):
        os.remove(filepath)

    with open(filepath, 'w') as f:
        f.writelines(rows)

    print('********End*********')


get_exchange_rate = PythonOperator(
    task_id="get_exchange_rate",
    python_callable=get_exchange_rate,
    templates_dict={
        'tmp_file_path': const_filepath,
        'target_date': '{{ ds }}',
        'app_id': app_id
    },
    provide_context=True,
    dag=dag
)

# MySQL表的结构如下:
# CREATE TABLE `exchange_rate` (
#  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
#  `base` varchar(16) NOT NULL DEFAULT '',
#  `date` varchar(16) NOT NULL DEFAULT '',
#  `currency` varchar(16) NOT NULL DEFAULT '',
#  `rate` decimal(18,6) NOT NULL,
#  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
#  PRIMARY KEY (`id`)
# )

load_exchange_rate = MySqlOperator(
    task_id="load_exchange_rate",
    sql=f"load data local infile '{const_filepath}' into table exchange_rate FIELDS TERMINATED BY ',' IGNORE 1 lines (base,date,currency,rate)",
    mysql_conn_id='mysql_default',
    autocommit=True,
    database='airflow',
    dag=dag
)

# CREATE TABLE `exchange_rate_summary` (
#  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
#  `stat_date` varchar(16) NOT NULL DEFAULT '',
#  `currency` varchar(16) NOT NULL DEFAULT '',
#  `min_rate` decimal(18,6) NOT NULL,
#  `max_rate` decimal(18,6) NOT NULL,
#  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
#  PRIMARY KEY (`id`)
# )

mysql2mysql_task = MySQLToMySQLOperator(
    task_id='mysql2mysql_task',
    destination_table='exchange_rate_summary',
    source_conn_id='mysql_default',
    destination_conn_id='mysql_default',
    sql="select current_date, currency, min(rate), max(rate) from exchange_rate where date >= date_sub(current_date, interval 7 day) group by currency",
    target_fields=['stat_date', 'currency', 'min_rate', 'max_rate'],
    dag=dag
)

t1 >> t2
t3.set_upstream(t1)

[t2, t3] >> get_exchange_rate

get_exchange_rate >> load_exchange_rate >> mysql2mysql_task

# 演示通过代码生产Task, 注意效率, 不能访问外部资源
tail_task = load_exchange_rate
for i in range(2):
    dummy_task = DummyOperator(
        task_id=f'dummy_task{i}',
        dag=dag
    )

    dummy_task.set_upstream(tail_task)
    tail_task = dummy_task

if __name__ == '__main__':
    dag.cli()

