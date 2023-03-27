import pendulum
import os
from airflow.models import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.sensors.python import PythonSensor
import pandas as pd
import mysql.connector as sql
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.providers.mysql.hooks.mysql import MySqlHook


def check_file():
    return os.path.isfile("/opt/airflow/content/tiktok_google_play_reviews.csv")

def load():
    data = pd.read_csv("/opt/airflow/content/tiktok_google_play_reviews.csv")

def transform():
    data["content"]=data["content"].str.replace(r'(?:[\U00000800-\U0010FFFF])+', "", regex=True)
    data = data.fillna('-')
    data = data.sort_values(by="at")


with DAG(
        dag_id="file_mongo_dag1",
        schedule=None,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False
) as dag:
    # t1 = FileSensor(task_id="wait_for_file", filepath="/opt/airflow/dags/test.txt")
    t1 = PythonSensor(task_id="sensor_python", python_callable=check_file)
    t2 = PythonOperator(task_id='load', python_callable=load)
    t3 = PythonOperator(task_id='transform', python_callable=transform)

    t1 >> t2 >> t3
