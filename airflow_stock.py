from datetime import datetime, timedelta

from airflow import DAG
from airflow.utils import dates
from airflow.utils.dates import days_ago
from airflow.utils.helpers import chain
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook


default_args = {
    'owner': 'airflow',
    'start_date': dates.days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}


def InsertData():
    mysql_hook = MySqlHook(mysql_conn_id="mysql_lhq")
    cnx = mysql_hook.get_conn()
    cur_test = cnx.cursor()
    current_time = str(datetime.today())
    insert_sql = f"insert into test(info) values('{current_time}')"
    cur_test.execute(insert_sql)
    cnx.commit()
    cur_test.close()
    cnx.close()
    print(f"当前时间为：{current_time}")

def task(dag):
    task = PythonOperator(
        task_id='InsertTdx',
        python_callable=InsertData,
        dag=dag
    )
    return task


d = DAG(
    'tutorial_test',
    default_args=default_args,
    description='定时写入aliyun的DAG',
    schedule_interval="22 14 * * *",
    #schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['example_lhq'],
)

task1 = task(d)
chain(task1)
