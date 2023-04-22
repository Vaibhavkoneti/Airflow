from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os

def python_first_function():
    return str(datetime.now())

default_dag_args = {
    'start_date' : datetime(2023, 4, 21),
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5),
    'project_id' : 1
}

with DAG("Second_DAG",schedule_interval = None,default_args = default_dag_args) as dag:
    task_0 = PythonOperator(task_id = "first_python_task", python_callable = python_first_function)

task_0