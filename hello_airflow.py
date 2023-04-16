import datetime
from datetime import timedelta
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.utils.helpers import chain


def default_options():
    default_args = {
        'owner': 'lihq',
        'start_date': days_ago(1),
        'retries': 1,
        'retry_delay': timedelta(seconds=5)
    }
    return default_args


def task1(dag):
    t = "pwd"
    task = BashOperator(
        task_id='MyTask1',
        bash_command=t,
        dag=dag
    )
    return task


def hello_world():
    current_time = str(datetime.datetime.today())
    print(f'大家好，现在的时间是{current_time}')


def task2(dag):
    task = PythonOperator(
        task_id="MyTask2",
        python_callable=hello_world,
        dag=dag
    )
    return task


def task3(dag):
    t = 'date'
    task = BashOperator(
        task_id='MyTask3',
        bash_command=t,
        dag=dag
    )
    return task

default_args = {
    'owner': 'lihq',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

dag = DAG(
    'HelloworldDag',
    default_args=default_args,
    description='我的第一个DAG',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['example_lhq'],
)

# with DAG(
#     'HelloworldDag',
#     default_args=default_options,
#     schedule_interval="* * * * *"
# ) as d:
task1 = task1(dag)
task2 = task2(dag)
task3 = task3(dag)
chain(task1, task2, task3)


