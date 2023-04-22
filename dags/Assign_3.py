from airflow import DAG
import requests
import time
import json
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os

def get_data(tickers):
    api_key = 'SRIRC9YRLR7C4N7G'
    for value in tickers:
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={value}&apikey=' + api_key
        r = requests.get(url)
        try:
            data = r.json()
            path = "/opt/airflow/data/Data_lake/stock_raw_data"
            with open(path + "stock_M_raw_data"+str(value)+str(time.time()),"w") as outfile:
                json.dump(data,outfile)
        except:
            pass

default_dag_args = {
    'start_date' : datetime(2023, 4, 21),
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5),
    'project_id' : 1
}

with DAG("Third_DAG",schedule_interval = None,default_args = default_dag_args) as dag:
    task_0 = PythonOperator(task_id = "get_market_data", python_callable = get_data, op_kwargs = {'tickers' : ['IBM']})
task_0