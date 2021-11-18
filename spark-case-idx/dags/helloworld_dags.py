from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import datetime as dt

def hello():
    print("Hello World")
    return "Hai You"

def say(message):
    print(message)
    
    return f"this is message {message}"
    
with DAG(dag_id='first_dag', 
         description='my first dag', 
         schedule_interval='10 * * * *', 
         start_date=dt.datetime(2021, 11, 12),
         dagrun_timeout=dt.timedelta(minutes=60)) as dag:
    start = DummyOperator(task_id='start_task', retries=3)
    finish = DummyOperator(task_id='finish_task', retries=3)

    main = PythonOperator(task_id='main_task', python_callable=hello, dag=dag, retries=2)
    # msg = task_instance.xcom_pull(task_ids='main_task')
    
    msg = "{{ ti.xcom_pull(task_ids='main_task') }}"
    
    main2 = PythonOperator(task_id='main2_task', python_callable=say, 
                           op_kwargs={'message': msg }, dag=dag, retries=2)

    start >> main >>main2 >> finish