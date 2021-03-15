from pyspark import SparkContext, SQLContext
from configparser import ConfigParser
import os
import datetime as dt
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator



# Config
os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--driver-class-path app/driver/postgresql-42.2.19.jar --jars app/driver/postgresql-42.2.19.jar pyspark-shell'
logging.basicConfig(
    filename='../logs/{}.log'.format(str(dt.datetime.now()).translate(str.maketrans({'-': '', ':': '', ' ': '', '.': ''}))),
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s')

# Initialize
sc = SparkContext()
spark = SQLContext(sc)
logger = logging.getLogger()
data = 'app/datafile/data-daftar-harga-bahan-pokok-di-provinsi-dki-jakarta-januari-2020.csv'


def read_file(csv_file):
    df = spark.read.csv(csv_file)
    try:
        df = spark.read.csv(csv_file)
        logger.info('csv success read')
        return df
    except Exception as e:
        logger.error(e)
        return e

def postgres_conn():
    db_properties = {}
    config = ConfigParser()
    config.read("app/db_config.ini")
    db_prop = config['DBDestination']
    db_properties['url'] = db_prop['url']
    db_properties['user'] = db_prop['user']
    db_properties['password'] = db_prop['password']
    # db_properties['url'] =

    db_properties['driver'] = db_prop['driver']

    return db_properties


def insert_bahan_pokok():
    db_properties = postgres_conn()
    df = read_file(data)
    # _select_sql = ""
    try:
        df_select = df.write.mode('append').jdbc(url=db_properties['url'], table="d_bahan_pokok",
                                                 properties=db_properties)
        logger.info('Success to insert Bahan Pokok')
        return df_select
    except Exception as e:
        logger.error(e)
        return e



with DAG(dag_id='bahan_pokok', description='insert bahan pokok dag',
         schedule_interval='21 13 * * *',
         start_date=dt.datetime(2020, 4, 23),
         dagrun_timeout=dt.timedelta(minutes=60)) as dag:
    start = DummyOperator(task_id='start_task', retries=3)
    finish = DummyOperator(task_id='finish_task', retries=3)

    main_insert_bahan_pokok = PythonOperator(task_id='main_task', python_callable=insert_bahan_pokok, dag=dag, retries=2)

    start >> main_insert_bahan_pokok >> finish