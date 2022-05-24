import datetime as dt
import requests
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2022, 5, 24, 0, 30),
    'depends_on_past': False,
}

def insert_data():
    # getting data
    url = 'http://api.open-notify.org/iss-now.json'
    response = requests.get(url, stream=True)
    response.raise_for_status()
    data = response.json()

    # creating a connection and inserting data into the database
    try:
        query = "INSERT INTO STATION_COORDINATES (TIMESTAMP, MESSAGE, latitude, LONGITUDE) " \
                "VALUES ('{0}', '{1}', '{2}', '{3}');" \
            .format(data['timestamp'], data['message'], data['iss_position']['latitude'], data['iss_position']['longitude'])

        pg_hook = PostgresHook(postgres_conn_id='postgres_db', shema='analytics')
        pg_hook.log.info(data['timestamp'])
        pg_hook.run(query)

    except Exception as e:
        pg_hook.log.info('An exception occurred with following data: {0}'.format(data))
        return 1

with DAG(
        dag_id='getting_station_coordinates',
        default_args=args,
        schedule_interval="*/30 * * * *"
) as dag:
    start_task = DummyOperator(task_id='start_task')
    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id='postgres_db',
        sql="""
            CREATE TABLE IF NOT EXISTS STATION_COORDINATES (
                TIMESTAMP   INTEGER PRIMARY KEY NOT NULL,
                MESSAGE     VARCHAR,
                LATITUDE    VARCHAR,
                LONGITUDE   VARCHAR);
          """,
    )
    insert_data = PythonOperator(
        task_id='insert_data',
        python_callable=insert_data,
        dag=dag
    )
    start_task >> create_table >> insert_data