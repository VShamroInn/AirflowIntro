import pendulum
import os
from airflow.models import DAG
from airflow.sensors.python import PythonSensor
import pandas as pd
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.decorators import task_group


def check_file():
    return os.path.isfile("/opt/airflow/content/files/tiktok_google_play_reviews.csv")


def load(**kwargs):
    data = pd.read_csv("/opt/airflow/content/files/tiktok_google_play_reviews.csv")
    kwargs['ti'].xcom_push(key='data', value=data.to_dict())


def transform1(**kwargs):
    data = pd.DataFrame.from_dict(kwargs['ti'].xcom_pull(task_ids="load", key="data"))
    data["content"] = data["content"].str.replace(r'(?:[\U00000800-\U0010FFFF])+', "", regex=True)
    kwargs['ti'].xcom_push(key='trdata', value=data.to_dict())


def transform2(**kwargs):
    data = pd.DataFrame.from_dict(kwargs['ti'].xcom_pull(task_ids="group3.transform1", key="trdata"))
    data = data.fillna('-')
    kwargs['ti'].xcom_push(key='trdata', value=data.to_dict())


def transform3(**kwargs):
    data = pd.DataFrame.from_dict(kwargs['ti'].xcom_pull(task_ids="group3.transform2", key="trdata"))
    data = data.sort_values(by="at")
    kwargs['ti'].xcom_push(key='trdata', value=data.to_dict())


def upload(**kwargs):
    data = pd.DataFrame.from_dict(kwargs['ti'].xcom_pull(task_ids="group3.transform3", key="trdata"))
    mysqlserver = MySqlHook("MySql1")
    create_table_query = """CREATE TABLE IF NOT EXISTS first.info (
        reviewId VARCHAR(128), 
        userName VARCHAR(255), 
        userImage VARCHAR(255), 
        content VARCHAR(2048), 
        score VARCHAR(8), 
        thumbsUpCount VARCHAR(16), 
        reviewCreatedVersion VARCHAR(16), 
        at VARCHAR(32), 
        replyContent VARCHAR(2048), 
        repliedAt VARCHAR(128))"""
    mysqlserver.run("CREATE DATABASE IF NOT EXISTS first")
    mysqlserver.run(create_table_query)
    mysqlserver.insert_rows(table="first.info", rows=data.values.tolist())


with DAG(
        dag_id="file_to_mysql_xcom",
        schedule=None,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False
) as dag:
    t1 = PythonSensor(task_id="sensor_python", python_callable=check_file)
    t2 = PythonOperator(task_id='load', python_callable=load)


    @task_group()
    def group3():
        t31 = PythonOperator(task_id='transform1', python_callable=transform1)
        t32 = PythonOperator(task_id='transform2', python_callable=transform2)
        t33 = PythonOperator(task_id='transform3', python_callable=transform3)
        t31 >> t32 >> t33


    t4 = PythonOperator(task_id='upload', python_callable=upload)

    t1 >> t2 >> group3() >> t4
