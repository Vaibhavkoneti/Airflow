from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime, timedelta
import os

create_query = """
CREATE TABLE IF NOT EXISTS employees (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    age INTEGER NOT NULL
);
"""
insert_data_query = """
INSERT INTO employees (name, age) VALUES
    ('Vaibhav koneti', 26),
    ('Barnabas', 25),
    ('Onur', 26);
"""
calculating_average_age = """
SELECT AVG(age) FROM employees;
"""

default_dag_args = {
    'start_date' : datetime(2023, 4, 21),
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5),
    'project_id' : 1
}

with DAG("Fourth_DAG",schedule_interval = None,default_args = default_dag_args) as dag:
    create_table = PostgresOperator(task_id = "creation_of_table", sql = create_query, postgres_conn_id = "postgres_vaibhav_local")
    insert_data = PostgresOperator(task_id = "insertion_of_data", sql = insert_data_query, postgres_conn_id = "postgres_vaibhav_local")
    group_data = PostgresOperator(task_id = "calculating_averag_age", sql = calculating_average_age, postgres_conn_id = "postgres_vaibhav_local")

create_table >> insert_data >> group_data
