from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from sqlalchemy import create_engine
import pandas as pd
import json

conn_str = "postgresql+psycopg2://airflow:airflow@postgres/airflow"

engine = create_engine(conn_str)
CON = engine.connect()
URL = 'https://statsapi.web.nhl.com/api/v1/teams/21/stats'


def extract_data(url, tmp_file, **context):
    pd.read_json(url).to_json(tmp_file)


def insert_to_db(tmp_file, table_name, conn=CON, **context):
    with open(tmp_file) as f:
        d = json.load(f)

    data = []
    for key, value in d['stats'].items():
        mini_dataset = {'team_id':      value['splits'][0]['team'].get('id', 'null'),
                        'team_name':    value['splits'][0]['team'].get('name', 'null'),
                        'team_link':    value['splits'][0]['team'].get('link', 'null'),
                        'gamesPlayed':  value['splits'][0]['stat'].get('gamesPlayed', 'null'),
                        'wins':         value['splits'][0]['stat'].get('wins', 'null'),
                        'losses':       value['splits'][0]['stat'].get('losses', 'null'),
                        'ot':           value['splits'][0]['stat'].get('ot', 'null'),
                        'pts':          value['splits'][0]['stat'].get('pts', 'null'),
                        'ptPctg':       value['splits'][0]['stat'].get('ptPctg', 'null'),
                        'goalsPerGame': value['splits'][0]['stat'].get('goalsPerGame', 'null'),
                        'displayName':  value['type']['displayName'],
                        'gameType_id':  value['type']['gameType']['id'] if value['type']['gameType'] is not None else 'null',
                        'description':  value['type']['gameType']['description'] if value['type']['gameType'] is not None else 'null',
                        'postseason':   value['type']['gameType']['postseason'] if value['type']['gameType'] is not None else 'null'}

        data.append(mini_dataset)

    data = pd.DataFrame(data)

    data["insert_time"] = pd.to_datetime("now")
    data.to_sql(table_name, conn, if_exists="replace")


dag = DAG(dag_id='dag_NHL',
          default_args={'owner': 'airflow'},
          schedule_interval='0 */12 * * *',
          start_date=days_ago(1))

extract_data = PythonOperator(
    task_id='extract_data_from_api',
    python_callable=extract_data,
    dag=dag,
    op_kwargs={
        'url': URL,
        'tmp_file': '/tmp/api_data_nhl'
    }
)

insert_data_into_db = PythonOperator(
    task_id='insert_data_into_db',
    python_callable=insert_to_db,
    dag=dag,
    op_kwargs={
        'table_name': 'api_data_nhl',
        'tmp_file': '/tmp/api_data_nhl',
        'conn': CON
    }
)
extract_data >> insert_data_into_db
