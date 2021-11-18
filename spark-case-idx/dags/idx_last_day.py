from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import datetime as dt
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import functions as F
import json
import configparser
import os
import re

# set global vars
spark = SparkSession.builder.appName('idx_last_day')\
    .config("spark.jars", "/opt/airflow/jars/postgresql-42.3.1.jar")\
    .getOrCreate()
out_path = f"/opt/airflow/log_output/idx-last-date-{dt.datetime.today().strftime('%Y%m%d-%I%M')}"
table_name = "idx_last_day"

def schema_df_config(df_pyspark):
    """
        Function for set schema of dataframe
    """
    # Change Schema Type of Dataframe and replace ","
    coltype_map = {
        "name" : StringType(),
        "prev" : IntegerType(),
        "open" : IntegerType(),
        "high" : IntegerType(),
        "low" : IntegerType(),
        "close" : IntegerType(),
        "change" : IntegerType(),
    }
    
    df_pyspark_col = df_pyspark.schema.names
    print(df_pyspark_col)
    for key, val in coltype_map.items():
        if key in df_pyspark_col:
            df_pyspark = df_pyspark.withColumn(f'{key}', F.regexp_replace(f'{key}', ",", ""))
            df_pyspark = df_pyspark.withColumn(f'{key}', df_pyspark[f"{key}"].cast(val))
    
    return df_pyspark


def get_file_input(**kwargs):
    """
        Function for get filename as input to create dataframe
    """
    path = "/opt/airflow/input_files"
    files = os.listdir(path)
    filename = list(filter((lambda x: re.search(r'.json', x)),files))[0]
    path_filename = f"{path}/{filename}"
    kwargs['ti'].xcom_push(key='idx_input_filename', value=path_filename)

def create_dataframe(filename, **kwargs):
    # Create dataframe from json file 
    df_pyspark = spark.read\
        .option("multiline","true")\
        .json(filename)\
        .select("name", "prev", "open", "high", "low", "close", "change")
    
    # Chenge Schema Type of Dataframe and replace ","
    df = schema_df_config(df_pyspark)
    
    # Remove rows if any null values
    df = df_pyspark.na.drop()
    
    # save to csv and push filename to Xcom
    df.write.option("header",True).csv(f"{out_path}.csv")
    # df.toPandas().to_csv(f"{out_path}.csv")
    kwargs['ti'].xcom_push(key='idx_out_csv', value=f"{out_path}.csv")

def db_properties(**kwargs):
    #Create the Database properties
    db_properties={}
    config = configparser.ConfigParser()
    config.read("/opt/airflow/db_properties.ini")
    db_prop = config["postgresql"]
    db_url = db_prop["url"]
    db_properties["user"]=db_prop["username"]
    db_properties["password"]=db_prop["password"]
    db_properties["url"]=db_prop["url"]
    db_properties["driver"]=db_prop["driver"]

    kwargs['ti'].xcom_push(key='db_prop', value=json.dumps(db_properties))

def write_to_jdbc(df, db_properties,  table_name):
    # Read dataframe
    df = spark.read\
        .options(header='True', inferSchema='True')\
        .csv(df)\
        .select("name", "prev", "open", "high", "low", "close", "change")
    
    # update schema
    # Chenge Schema Type of Dataframe and replace ","
    df = schema_df_config(df)
    
    # load dict db_propeties
    db_properties = json.loads(db_properties)
    #Save the dataframe to the JDBC table.
    df.write.mode('overwrite').jdbc(url=db_properties['url'], table="idx_last_day", properties=db_properties)
    print("Succes to write data")


with DAG(dag_id='idx_last_day', 
         description='ingest idx last day to postgres',
         schedule_interval='0 0 1 1 *', 
         start_date=dt.datetime(2021, 11, 15),
         dagrun_timeout=dt.timedelta(minutes=60)) as dag:
    
    start = DummyOperator(task_id='start_task')
    finish = DummyOperator(task_id='finish_task')
    
    get_file_input = PythonOperator(task_id='get_file_input', python_callable = get_file_input, dag=dag, retries=3)
    input_file_name = "{{ ti.xcom_pull(key='idx_input_filename') }}"
                 
    db_properties = PythonOperator(task_id='load_db_properties', python_callable = db_properties, dag=dag, retries=3)
    db_prop = ("{{ ti.xcom_pull(key='db_prop') }}")    
    
    create_dataframe = PythonOperator(task_id='create_dataframe', 
                                      python_callable = create_dataframe, 
                                      op_kwargs={'filename': input_file_name }, 
                                      dag=dag, 
                                      retries=3,
                                      retry_delay=dt.timedelta(minutes=1))   
    df = "{{ ti.xcom_pull(key='idx_out_csv') }}"

    
    write_to_jdbc = PythonOperator(task_id ='idx_last_date_jdbc',
                                   python_callable = write_to_jdbc,
                                   op_kwargs={'df':df, 'db_properties':db_prop,  'table_name':table_name},
                                   dag=dag,
                                   retries=3
                                  )

    start >> [db_properties, get_file_input] >> create_dataframe >>write_to_jdbc >> finish 
    
