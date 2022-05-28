import logging

import requests
from datetime import datetime
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

COORDINATES_API_URL = 'http://api.open-notify.org/iss-now.json'

def _get_data(**context):
    api_req = requests.request("GET", COORDINATES_API_URL,
                               headers={'Content-Type': 'application/json; charset=utf-8'}, data={})

    api_json = api_req.json()
    if api_json['message'] == 'success':
        context["task_instance"].xcom_push(key="api_json", value=api_json)
    else:
        logging.info('Getting failed data: {0}'.api_json)
    return 1


def _parse_data(**context):
    api_json = context["task_instance"].xcom_pull(
        task_ids="get_data", key="api_json")
    api_data = [api_json['timestamp'], api_json['message'],
                api_json['iss_position']['latitude'], api_json['iss_position']['longitude']]
    context["task_instance"].xcom_push(key="api_data", value=api_data)


def _insert_data(**context):
    api_data = context["task_instance"].xcom_pull(
        task_ids="parse_data", key="api_data")
    dest = PostgresHook(postgres_conn_id='postgres_db')
    logging.info(api_data)
    dest_conn = dest.get_conn()
    dest_cursor = dest_conn.cursor()
    dest_cursor.execute(
        """INSERT INTO station_data(timestamp, message, latitude, longitude) VALUES (%s, %s, %s, %s)""", (api_data))
    dest_conn.commit()


args = {'owner': 'airflow'}

dag = DAG(
    dag_id="get_station_coordinates",
    default_args=args,
    start_date=datetime(2021, 7, 1),
    schedule_interval='*/30 * * * *',
    catchup=False,
)


start = DummyOperator(task_id="start", dag=dag)

get_data = PythonOperator(
    task_id='get_data', python_callable=_get_data, provide_context=True, dag=dag)
parse_data = PythonOperator(
    task_id='parse_data', python_callable=_parse_data, provide_context=True, dag=dag)
insert_data = PythonOperator(
    task_id='insert_data', python_callable=_insert_data, provide_context=True, dag=dag)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id="postgres_db",
    sql='''CREATE TABLE IF NOT EXISTS station_data(
            date_load     timestamp NOT NULL DEFAULT NOW()::timestamp,
            timestamp     integer PRIMARY KEY NOT NULL,
            message       varchar,
            latitude      decimal,
            longitude     decimal);
            ''', dag=dag
)


start >> create_table >> get_data >> parse_data >> insert_data