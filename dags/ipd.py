import os
from datetime import datetime
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.email_operator import EmailOperator

def read_sql():
    pg_hook = PostgresHook(postgres_conn_id='airflow_db')
    sql_file = open("./dags/sql/select_covid19.sql")
    sql_as_string = sql_file.read()
    if(sql_as_string):
        pg_hook.run(sql_as_string)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 2, 18),
    'schedule_interval': None,
}
with DAG('IPD_order',
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False) as dag:
	
    t1 = PythonOperator(
        task_id='read_sql',
        python_callable=read_sql
    )

    t3 = EmailOperator(
        task_id='send_email',
        trigger_rule=TriggerRule.ONE_FAILED,
        to=['s6103052422071@email.kmutnb.ac.th'],
        subject='Your IPD_order report today is failed',
        html_content='Please check your dashboard. :)'
    )

    t1 >> t3