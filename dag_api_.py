import pendulum
import requests
from airflow.decorators import task, dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os
import tempfile

# SQL для создания таблицы
sql_schema_init = """
CREATE TABLE IF NOT EXISTS public.cannabis_products (
    id SERIAL PRIMARY KEY,
    uid TEXT,
    strain TEXT,
    cannabinoid_abbreviation TEXT,
    cannabinoid TEXT,
    terpene TEXT,
    medical_use TEXT,
    health_benefit TEXT,
    category TEXT,
    type TEXT,
    buzzword TEXT,
    brand TEXT
);
"""

@dag(
    'dag_api_',
    schedule_interval='0 */12 * * *',
    tags=['api'],
    start_date=pendulum.datetime(2024, 8, 30),
    catchup=False,
    default_args={
        'owner': 'andrew'
    }
)
def dag_api():
    # Операция создания схемы
    schema_init = PostgresOperator(
        task_id='schema_init',
        postgres_conn_id='api_db',
        sql=sql_schema_init
    )

    @task
    def transfer_data_api(**context):
        url = 'https://random-data-api.com/api/cannabis/random_cannabis?size=10'
        response = requests.get(url)
        response.encoding = 'utf-8'
        records = response.json()

        # Формирование данных для вставки в таблицу
        out = ''
        for record in records:
            line = f"{record['id']}\t{record['uid']}\t{record['strain']}\t{record['cannabinoid_abbreviation']}\t{record['cannabinoid']}\t"
            line += f"{record['terpene']}\t{record['medical_use']}\t{record['health_benefit']}\t{record['category']}\t"
            line += f"{record['type']}\t{record['buzzword']}\t{record['brand']}\n"
            out += line

        # Запись данных во временный файл
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as _tmp:
            _tmp.write(out)
            tmp_file = _tmp.name

        # Загрузка данных в PostgreSQL
        try:
            hook = PostgresHook(postgres_conn_id='api_db')
            hook.bulk_load('cannabis_products', tmp_file)
        finally:
            os.remove(tmp_file)

    schema_init >> transfer_data_api()

mydag = dag_api()
