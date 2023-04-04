import pendulum
import os
from airflow.models import DAG
from airflow.sensors.python import PythonSensor
import pandas as pd
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.decorators import task_group


def check_file():
    return os.path.isfile("/opt/airflow/content/files/tiktok_google_play_reviews.csv")


def transform1(**kwargs):
    data = pd.read_csv("/opt/airflow/content/files/tiktok_google_play_reviews.csv")
    data["content"] = data["content"].str.replace(r'(?:[\U00000800-\U0010FFFF])+', "", regex=True)
    data.to_csv('/opt/airflow/content/dag_steps/data_step_1.csv', index=False)


def transform2(**kwargs):
    data = pd.read_csv("/opt/airflow/content/dag_steps/data_step_1.csv")
    data = data.fillna('-')
    data.to_csv('/opt/airflow/content/dag_steps/data_step_2.csv', index=False)


def transform3(**kwargs):
    data = pd.read_csv("/opt/airflow/content/dag_steps/data_step_2.csv")
    data = data.sort_values(by="at")
    data.to_csv('/opt/airflow/content/dag_steps/data_step_3.csv', index=False)


def upload(**kwargs):
    data = pd.read_csv("/opt/airflow/content/dag_steps/data_step_3.csv")
    mongoserv = MongoHook("Mongocon")
    mongoserv.insert_many('info',data.to_dict(orient="records"),'first')


with DAG(
        dag_id="file_to_mongo",
        schedule=None,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False
) as dag:
    t1 = PythonSensor(task_id="sensor_python", python_callable=check_file)

    @task_group()
    def group2():
        t31 = PythonOperator(task_id='transform1', python_callable=transform1)
        t32 = PythonOperator(task_id='transform2', python_callable=transform2)
        t33 = PythonOperator(task_id='transform3', python_callable=transform3)
        t31 >> t32 >> t33


    t3 = PythonOperator(task_id='upload', python_callable=upload)

    t1 >> group2() >> t3
