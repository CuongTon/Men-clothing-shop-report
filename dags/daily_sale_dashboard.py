from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
from pymongo import MongoClient 
from group_dags.subdag_download import download_task
from group_dags.check_log_in import check_log_in_available
from group_dags.ETL_daily_sale import ETL_daily_sale
import sys
sys.path.append('/home/cuongton/airflow/')
from project_setting import time_setting, generall_setting

def _is_the_first_time():
    server = MongoClient("mongodb://192.168.1.20:27017")
    db = server[generall_setting.Mongo_Database]
    collection = db[generall_setting.Mongo_Collection]
    if collection.find_one():
        return 'incremental_load'
    else:
        return 'initial_load'

with DAG(
    "daily_sale_dashboard",
    schedule_interval='0 9 * * *',
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

    download_raw_data = download_task()

    check_log_in = check_log_in_available()

    merge_and_put_S3 = BashOperator(
        task_id = "merge_and_put_S3",
        bash_command="""
            source ~/airflow/bin/activate
            cd /home/cuongton/airflow/project_code/spark_app_etl
            python3 Merge_and_Put_S3.py
        """
    )

    replica_task = BashOperator(
        task_id = 'replica_task',
        bash_command="""
            source ~/airflow/bin/activate
            python3 /home/cuongton/airflow/project_code/spark_app_etl/Replica_source.py
        """
    )

    is_the_first_time = BranchPythonOperator(
        task_id = 'is_the_first_time',
        python_callable=_is_the_first_time
    )

    initial_load = SparkSubmitOperator(
        task_id = 'initial_load',
        conn_id='local_spark',
        application='/home/cuongton/airflow/project_code/spark_app_etl/Initial_load.py',
        packages='org.apache.hadoop:hadoop-aws:3.3.1,org.apache.hadoop:hadoop-common:3.3.1,org.mongodb.spark:mongo-spark-connector_2.12:3.0.2'
    )

    incremental_load = SparkSubmitOperator(
        task_id = 'incremental_load',
        conn_id='local_spark',
        application='/home/cuongton/airflow/project_code/spark_app_etl/Incremental_load.py',
        packages='org.apache.hadoop:hadoop-aws:3.3.1,org.apache.hadoop:hadoop-common:3.3.1,org.mongodb.spark:mongo-spark-connector_2.12:3.0.2'
    )

    daily_sale_data_mart = ETL_daily_sale()

    start >> check_log_in >> download_raw_data >> merge_and_put_S3 >> is_the_first_time >> [initial_load, incremental_load] >> daily_sale_data_mart >> end
    download_raw_data >> replica_task >> end

