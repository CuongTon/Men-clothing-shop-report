from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
from pymongo import MongoClient 
import sys
sys.path.append('/home/cuongton/airflow/')
from project_setting import time_setting

def _is_the_first_time():
    server = MongoClient("mongodb://192.168.1.20:27017")
    db = server['ShopeeVN_airflow']
    collection = db['KraftVN_airflow']
    if collection.find_one():
        return 'incremental_load'
    else:
        return 'initial_load'

with DAG(
    "daily_sale_dashboard",
    schedule_interval='@daily',
    catchup=False,
    start_date=time_setting.start_time
) as dag:
    
    start = EmptyOperator(
        task_id = 'start'
    )

    end = EmptyOperator(
        task_id = 'end',
        trigger_rule='none_failed'
    )

    Kraft_data_from_shopee = BashOperator(
        task_id = 'Kraft_data_from_shopee',
        bash_command='''
            source ~/airflow/bin/activate
            cd /home/cuongton/airflow/project_code/crawl_shopee_data
            scrapy crawl Kraftvn
        '''
    )

    is_the_first_time = BranchPythonOperator(
        task_id = 'is_the_first_time',
        python_callable=_is_the_first_time
    )

    initial_load = SparkSubmitOperator(
        task_id = 'initial_load',
        conn_id='local_spark',
        application='/home/cuongton/airflow/project_code/spark_app_etl/Kraft_initial_load.py',
        packages='org.apache.hadoop:hadoop-aws:3.3.1,org.apache.hadoop:hadoop-common:3.3.1,org.mongodb.spark:mongo-spark-connector_2.12:3.0.2'
    )

    incremental_load = SparkSubmitOperator(
        task_id = 'incremental_load',
        conn_id='local_spark',
        application='/home/cuongton/airflow/project_code/spark_app_etl/Kraft_incremental_load.py',
        packages='org.apache.hadoop:hadoop-aws:3.3.1,org.apache.hadoop:hadoop-common:3.3.1,org.mongodb.spark:mongo-spark-connector_2.12:3.0.2'
    )

    daily_sale_data_mart = BashOperator(
        task_id = 'daily_sale_data_mart',
        bash_command='''
            source ~/airflow/bin/activate
            python3 /home/cuongton/airflow/project_code/data_mart_etl/daily_sale_star_schema.py
        ''',
        trigger_rule = "none_failed"

    )

    start >> Kraft_data_from_shopee >> is_the_first_time >> [initial_load, incremental_load] >> daily_sale_data_mart >> end
    
