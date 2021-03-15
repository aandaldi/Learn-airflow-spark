from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import datetime as dt


def hello_world():
    print("Hello")
    return "hello"


with DAG(dag_id='first_dag', description='my first dag',
         schedule_interval='25 11 * * *',
         start_date=dt.datetime(2020, 4, 23),
         dagrun_timeout=dt.timedelta(minutes=60)) as dag:
    start = DummyOperator(task_id='start_task', retries=3)
    finish = DummyOperator(task_id='finish_task', retries=3)

    main = PythonOperator(task_id='main_task', python_callable=hello_world, dag=dag, retries=2)

    start >> main >> finish