from uuid import uuid4
from json import loads
from datetime import datetime, timedelta

import pandas as pd
from mysql.connector import Error
import mysql.connector as mysql

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator


from utils.api import fetch_api_key, call_api

default_args = {
   'owner': 'pratik'
}


def load(ti):
    result = ti.xcom_pull(task_ids='merge_transform')

    record_id, station_id, time_id, parameter_id, temp_id, heat_index_id = str(uuid4()), 1, str(uuid4()), str(uuid4()), str(uuid4()), str(uuid4()) 
    cxn = mysql.connect(host=Variable.get("MYSQL_HOST"), user=Variable.get("MYSQL_USER"), 
                        password=Variable.get("MYSQL_PSWD"), database=Variable.get("MYSQL_DB"))
    cur = cxn.cursor()

    try:
        cur.execute("INSERT INTO W_FACT(record_id, station_id, time_id, parameter_id, temp_id, heat_index_id)" 
                    f" VALUES ('{record_id}', {station_id}, '{time_id}', '{parameter_id}', '{temp_id}', '{heat_index_id}')")
        
        cur.execute("INSERT INTO W_PARAM_DIM(parameter_id, humidity, pressure, visibility, cloudiness, dew_point, wind_speed, wind_direction)" 
                    f" VALUES ('{parameter_id}', '{result['humidity']}', '{result['pressure']}'," 
                    f" '{result['visibility']}', '{result['cloudiness']}', '{result['dew_point']}', {result['wind_speed']}, {result['wind_direction']})")
        
        cur.execute("INSERT INTO W_TIME_DIM(time_id, record_datetime, record_date, record_month, record_year, record_quarter, record_season, record_weekday)" 
                    f" VALUES ('{time_id}', '{result['recorded_datetime_local']}', '{result['recorded_date_local']}', '{result['recorded_month_local']}'," 
                    f" '{result['recorded_year_local']}', '{result['recorded_quarter_local']}', '{result['recorded_season_local']}', '{result['recorded_weekday_local']}')")
        
        cur.execute("INSERT INTO W_TEMP_DIM(temp_id, temp, temp_range_min, temp_range_max, feels_like)" 
                    f" VALUES ('{temp_id}', '{result['temp']}', '{result['temp_min']}', '{result['temp_max']}'," 
                    f" '{result['feels_like']}')")
        
        cur.execute("INSERT INTO W_HEAT_INDEX_DIM(heat_index_id, heat_index, heat_index_category, description)" 
                    f" VALUES ('{heat_index_id}', '{result['heat_index']}', 1, '')")
    except Error:
        cxn.rollback()
        raise
    else:
        cxn.commit()

    cur.close()
    cxn.close()
    return None

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

    month = int(series["recorded_datetime_local"].strftime("%m"))
    if 1 <= month <= 3:
        series["recorded_quarter_local"] = "Q1"
    elif 4 <= month <= 6:
        series["recorded_quarter_local"] = "Q2"
    elif 7 <= month <= 9:
        series["recorded_quarter_local"] = "Q3"
    elif 10 <= month <= 12:
        series["recorded_quarter_local"] = "Q4"

    if 3 <= month <= 5:
        series["recorded_season_local"] = "Spring"
    elif 6 <= month <= 8:
        series["recorded_season_local"] = "Summer"
    elif 9 <= month <= 11:
        series["recorded_season_local"] = "Fall"
    elif month == 12 or 1 <= month <= 2:
        series["recorded_season_local"] = "Winter"
    
    series["recorded_weekday_local"] = series["recorded_datetime_local"].strftime("%A")
    series["recorded_month_local"] = series["recorded_datetime_local"].strftime("%B")
    series["recorded_year_local"] = series["recorded_datetime_local"].year

    series["daylight_duration_delta"] = series["sunset_datetime_local"] - series["sunrise_datetime_local"]
    series["daylight_duration_minutes"] = series["daylight_duration_delta"].seconds // 60
    series.drop(["daylight_duration_delta"], inplace=True)

    series["recorded_date_local"] = series["recorded_datetime_local"].strftime('%Y-%m-%d')
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


with DAG(
    dag_id = 'weatherflow_etl',
    default_args = default_args,
    start_date = datetime.now() + timedelta(minutes=15),
    schedule_interval = timedelta(minutes=5),
    template_searchpath = '/home/pratik/weatherflow/dags',
    catchup=False,
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
