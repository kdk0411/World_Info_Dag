from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from pandas import Timestamp

import requests
import pandas as pd
import logging

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

@task
def get_info():
    path = "https://restcountries.com/v3/all"
    response = requests.get(path)
    response.raise_for_status()
    return response.json()

@task
def get_data(data):
    countries_info = []
    for country_data in data:
        country_name = country_data.get("name", {}).get("official", "")
        population = country_data.get("population", 0)
        area = country_data.get("area", 0)

        countries_info.append({'country_name': country_name, 'population': population, 'area': area})
    return pd.DataFrame(countries_info)

@task
def load(schema, table, countries_info):
    logging.info("load started")
    cur = get_Redshift_connection()
    try:
        cur.execute('BEGIN;')
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
        cur.execute(f"""
CREATE TABLE {schema}.{table}(
    country varchar,
    population bigint,
    area float
);""")
        for index, row in countries_info.iterrows():
            sql = f"INSERT INTO {schema}.{table} VALUES ('{row['country_name']}', {row['population']}, {row['area']});"
            print(sql)
            cur.execute(sql)
        cur.execute("COMMIT;")
    except Exception as error:
        print(error)
        cur.execute("ROLLBACK")
        raise
    logging.info("load done")

with DAG(
    dag_id = 'UpdateWorld',
    start_date = datetime(2023,5,30),
    catchup = False,
    tags = ['API'],
    schedule = '30 6 * * 6'
) as dag:
    country_info = get_info()
    country_info_df = get_data(country_info)
    load("wnsldjqja", "World_info",country_info_df)