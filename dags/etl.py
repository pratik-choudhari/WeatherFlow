from json import loads, dumps
from datetime import datetime, timedelta

import pandas as pd

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

from utils.api import fetch_api_key, call_api

default_args = {
   'owner': 'pratik'
}


def poll_api():
    api_key = fetch_api_key()
    return call_api(api_key)


def enrich_datetime(ti):
    json_data = ti.xcom_pull(task_ids='poll_api')

    series = pd.Series(json_data)
    series["timestamp"] = int(series["timestamp"])
    series["sunset_ts"] = int(series["sunset_ts"])
    series["sunrise_ts"] = int(series["sunrise_ts"])

    series["recorded_datetime_local"] = datetime.fromtimestamp(series["timestamp"] + series["timezone"])
    series["sunset_datetime_local"] = datetime.fromtimestamp(series["sunset_ts"] + series["timezone"])
    series["sunrise_datetime_local"] = datetime.fromtimestamp(series["sunrise_ts"] + series["timezone"])

    series.drop(["timestamp", "sunset_ts", "sunrise_ts"], inplace=True)
    
    series["recorded_weekday_local"] = series["recorded_datetime_local"].strftime("%A")
    series["recorded_month_local"] = series["recorded_datetime_local"].strftime("%B")
    series["recorded_year_local"] = series["recorded_datetime_local"].year

    series["daylight_duration_delta"] = series["sunset_datetime_local"] - series["sunrise_datetime_local"]
    series["daylight_duration_minutes"] = series["daylight_duration_delta"].seconds // 60
    series.drop(["daylight_duration_delta"], inplace=True)

    series["recorded_datetime_local"] = series["recorded_datetime_local"].strftime('%Y-%m-%d %H:%M:%S')
    series["sunset_datetime_local"] = series["sunset_datetime_local"].strftime('%Y-%m-%d %H:%M:%S')
    series["sunrise_datetime_local"] = series["sunrise_datetime_local"].strftime('%Y-%m-%d %H:%M:%S')

    return series.to_json()

def add_calc_attributes(ti):
    json_data = ti.xcom_pull(task_ids='poll_api')

    series = pd.Series(json_data)
    new_attributes = {}
    new_attributes["dew_point"] = round(series["temp"] - ((100 - series["humidity"]) / 5), 2)

    series["temp_F"] = (series["temp"] * 1.8) + 32
    new_attributes["heat_index"] = 0.5 * (series["temp_F"] + 61.0 + ((series["temp_F"] - 68.0) * 1.2) + (series["humidity"] * 0.094))
    new_attributes["heat_index"] = round(((new_attributes["heat_index"] - 32) * 5) / 9, 2)

    return new_attributes

def merge_transform(ti):
    enriched_data = loads(ti.xcom_pull(task_ids='enrich_datetime'))
    new_attributes = ti.xcom_pull(task_ids='add_calc_attributes')

    result = {**enriched_data, **new_attributes}
    return result

def load(ti):
    result = ti.xcom_pull(task_ids='merge_transform')
    print(result)
    return None


with DAG(
    dag_id = 'weatherflow_etl',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    template_searchpath = '/home/pratik/weatherflow/dags',
    tags = ['etl']
) as dag:
    
    poll_api_task = PythonOperator(
        task_id='poll_api',
        python_callable=poll_api
    )

    enrich_datetime_task = PythonOperator(
        task_id='enrich_datetime',
        python_callable=enrich_datetime
    )

    add_calc_attributes_task = PythonOperator(
        task_id='add_calc_attributes',
        python_callable=add_calc_attributes
    )

    merge_transform_task = PythonOperator(
        task_id='merge_transform',
        python_callable=merge_transform
    )

    store_to_db_task = PythonOperator(
        task_id='load',
        python_callable=load
    )

poll_api_task >> [enrich_datetime_task, add_calc_attributes_task] >> merge_transform_task >> store_to_db_task
