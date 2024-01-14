from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.exceptions import AirflowException
from datetime import datetime
from pymongo import MongoClient 
from group_dags.subdag_download import download_task
from group_dags.check_log_in import check_log_in_available
from group_dags.ETL_daily_sale import ETL_daily_sale
import sys
sys.path.append('/home/cuongton/airflow/')
from project_setting import time_setting, generall_setting, shop_setting
import logging
import json

# set up log. Use Airflow log
logger = logging.getLogger(__name__)

def _is_the_first_time():

    # connect to the server, db, collection.
    server = MongoClient("mongodb://192.168.1.20:27017")
    db = server[generall_setting.Mongo_Database]
    collection = db[generall_setting.Mongo_Collection]

    # recheck whether the database has existed. If not existed, run initial load, otherwise, run incremental load
    if collection.find_one():
        return 'incremental_load'
    else:
        return 'initial_load'

def _check_quantity(ti):

    # get current date. This is used for declare file path.
    current_date = datetime.today()

    # Use this variable to throw an error if the crawled data saved on the local disk doesn't match total number of items from the API.
    error_check = False

    # loop all shops
    for shop in shop_setting.shop_details:
        
        # create path to read file that saved on the local disk.
        path = f"{generall_setting.folder_name_staging_layer}/{current_date.year}/{current_date.month}/{current_date.day}/{shop['name']}.json"

        with open(f'/home/cuongton/airflow/project_code/crawl_shopee_data/{path}') as file:
            # count total number of lines from a file saved on the local disk.
            count_lines = len(json.loads(file.read()))

            # get total amount of items from the API through XCOM.
            API_lines = ti.xcom_pull(key='return_value', task_ids=f"downloads.download_{shop['name']}")

            # recheck the condition and log it.
            if count_lines == int(API_lines) and count_lines != 0:
                logger.info(f"{'-'*20} Success: Shop name is {shop['name']}, Total items is {API_lines}, Total crawled items is {count_lines} {'-'*20}")
            else:
                logger.info(f"{'-'*20} Error: Shop name is {shop['name']}, Total items is {API_lines}, Total crawled items is {count_lines} {'-'*20}")
                error_check = True

    # final check. Throw an error if exists.
    if error_check:
        raise AirflowException(f"{'-'*20} Recheck your download tasks, there are some files that didn't match in quantity. {'-'*20}")
    else:
        logger.info(f'{"-"*20} Final check: The data was crawled successfully {"-"*20}')

with DAG(
    "daily_sale_dashboard",
    schedule_interval='0 9 * * *',
    catchup=False,
    start_date=time_setting.start_time
) as dag:
    
    # create an empty tasks. Start and End.
    start = EmptyOperator(
        task_id = 'start'
    )

    end = EmptyOperator(
        task_id = 'end',
        trigger_rule='none_failed'
    )

    # this task will download data from e-commerce website.
    download_raw_data = download_task()

    # before download, make sure the account has been logged in. If the account was signed out, then it will sign in.
    check_log_in = check_log_in_available()

    # check quantity.
    check_quantity = PythonOperator(
        task_id = 'check_quantity',
        python_callable=_check_quantity
    )

    # after successfully fetching data. Combine it and store it on S3.
    merge_and_put_S3 = BashOperator(
        task_id = "merge_and_put_S3",
        bash_command="""
            source ~/airflow/bin/activate
            cd /home/cuongton/airflow/project_code/spark_app_etl
            python3 Merge_and_Put_S3.py
        """
    )

    # crawled data will be stored first in the staging layer, which is located on the local disk, before being combined and stored on S3. 
    # On S3, turn on versioning.
    # On the local disk, to avoid accidentally changing data, create a replica of it.
    replica_task = BashOperator(
        task_id = 'replica_task',
        bash_command="""
            source ~/airflow/bin/activate
            python3 /home/cuongton/airflow/project_code/spark_app_etl/Replica_source.py
        """
    )

    # check whether run an initial load or an incremental load.
    is_the_first_time = BranchPythonOperator(
        task_id = 'is_the_first_time',
        python_callable=_is_the_first_time
    )

    # initial load - create new database, collection if not exist
    initial_load = SparkSubmitOperator(
        task_id = 'initial_load',
        conn_id='local_spark',
        application='/home/cuongton/airflow/project_code/spark_app_etl/Initial_load.py',
        packages='org.apache.hadoop:hadoop-aws:3.3.1,org.apache.hadoop:hadoop-common:3.3.1,org.mongodb.spark:mongo-spark-connector_2.12:3.0.2'
    )

    # Following initial load, all subsequent processing tasks will run using incremental load.
    incremental_load = SparkSubmitOperator(
        task_id = 'incremental_load',
        conn_id='local_spark',
        application='/home/cuongton/airflow/project_code/spark_app_etl/Incremental_load.py',
        packages='org.apache.hadoop:hadoop-aws:3.3.1,org.apache.hadoop:hadoop-common:3.3.1,org.mongodb.spark:mongo-spark-connector_2.12:3.0.2'
    )

    # ETL. To transfer and load data from datawarehous to data mart.
    daily_sale_data_mart = ETL_daily_sale()

    # Work flow
    start >> check_log_in >> download_raw_data >> check_quantity >> merge_and_put_S3 >> is_the_first_time >> [initial_load, incremental_load] >> daily_sale_data_mart >> end
    check_quantity >> replica_task >> end

