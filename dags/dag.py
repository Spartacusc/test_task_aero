from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from sqlalchemy import create_engine
import pandas as pd

conn_str = "postgresql+psycopg2://airflow:airflow@postgres/airflow"

engine = create_engine(conn_str)
CON = engine.connect()


def extract_data(url, tmp_file, **context):
    pd.read_json(url).to_json(tmp_file)


def insert_to_db(tmp_file, table_name, conn=CON, **context):
    data = pd.read_json(tmp_file)
    data["insert_time"] = pd.to_datetime("now")
    data.to_sql(table_name, conn, if_exists="append")


dag = DAG(dag_id='dag',
          default_args={'owner': 'airflow'},
          schedule_interval='0 */12 * * *',
          start_date=days_ago(1))

extract_data = PythonOperator(
    task_id='extract_data_from_api',
    python_callable=extract_data,
    dag=dag,
    op_kwargs={
        'url': 'https://random-data-api.com/api/cannabis/random_cannabis?size=10',
        'tmp_file': '/tmp/api_data'
    }
)

insert_data_into_db = PythonOperator(
    task_id='insert_data_into_db',
    python_callable=insert_to_db,
    dag=dag,
    op_kwargs={
        'table_name': 'api_data',
        'tmp_file': '/tmp/api_data',
        'conn': CON
    }
)
extract_data >> insert_data_into_db
