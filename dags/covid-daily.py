import json
from datetime import datetime

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.email_operator import EmailOperator
import requests

def get_covid19_report_today():
    url = 'https://covid19.th-stat.com/api/open/today'
    response = requests.get(url)
    data = response.json()

    with open('data.json', 'w') as f:
        json.dump(data, f)

    return data

def insert_data():
    with open('data.json') as f:
        data = json.load(f)

    print(data) 
    
    mysql_hook = PostgresHook(postgres_conn_id='airflow_db')

    insert = """
        INSERT INTO public.daily_covid19_reports
            (confirmed, recovered, hospitalized, deaths, new_confirmed, new_recovered, new_hospitalized, new_deaths, update_date, "source", dev_by, server_by)
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    mysql_hook.run(insert, parameters=(data['Confirmed'],
                                       data['Recovered'],
                                       data['Hospitalized'],
                                       data['Deaths'],
                                       data['NewConfirmed'],
                                       data['NewRecovered'],
                                       data['NewHospitalized'],
                                       data['NewDeaths'],
                                       datetime.strptime(data['UpdateDate'], '%d/%m/%Y %H:%M'),
                                       data['Source'],
                                       data['DevBy'],
                                       data['SeverBy']))
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 2, 17),
    'schedule_interval': None,
}
with DAG('covid19_daily',
         schedule_interval='@daily',
         default_args=default_args,
         description='A simple data pipeline for COVID-19 report',
         catchup=False) as dag:
	
    t1 = PythonOperator(
        task_id='get_covid19_report_today',
        python_callable=get_covid19_report_today
    )

    t2 = PythonOperator(
        task_id='insert_data',
        python_callable=insert_data
    )

    t3 = EmailOperator(
        task_id='send_email',
        to=['s6103052422071@email.kmutnb.ac.th'],
        subject='Your covid19 report today is ready',
        html_content='Please check your dashboard. :)'
    )

    t1 >> t2 >> t3