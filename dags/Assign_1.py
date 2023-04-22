from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import os

cwd = os.getcwd()

default_dag_args = {
    'start_date' : datetime(2023, 4, 21),
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5),
    'project_id' : 1
}

with DAG("First_DAG",schedule_interval = None,default_args = default_dag_args) as dag:
    task_0 = BashOperator(task_id = 'bash_task_0', bash_command = "echo 'command executedfrom from Bash'")
    task_1 = BashOperator(task_id = 'bash_task_move_data', bash_command = 'cp /opt/airflow/data/Data_lake/raw_data.txt /opt/airflow/data/Data_ware/')
    task_2 = BashOperator(task_id = 'remove_data_from', bash_command = 'rm /opt/airflow/data/Data_lake/raw_data.txt')

task_0 >> task_1 >> task_2


