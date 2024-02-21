from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from datetime import datetime
from group_dags.test.subdag_5_profiles_test import download_task
from group_dags.test.check_5_profile_log_in_test import check_log_in_available
import sys
sys.path.append('/home/cuongton/airflow/')
from project_setting import shop_setting, generall_setting
import json
import logging


# set up log
logger = logging.getLogger(__name__)

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
            # TBU

            if count_lines >= int(int(API_lines)*0.99) and count_lines != 0:
                logger.info(f"{'-'*20} Success: Shop name is {shop['name']}, Total items is {API_lines}, Total crawled items is {count_lines} {'-'*20}")
            else:
                logger.info(f"{'-'*20} Error: Shop name is {shop['name']}, Total items is {API_lines}, Total crawled items is {count_lines} {'-'*20}")
                error_check = True


    # final check. Throw an error if exists.
    if error_check:
        # logger.info('Recheck')
        raise AirflowException(f"{'-'*20} Recheck your download tasks, there are some files that didn't match in quantity. {'-'*20}")
    else:
        logger.info(f'{"-"*20} Final check: The data was crawled successfully {"-"*20}')


with DAG("test_5_download_pages", start_date=datetime(2023, 12, 31), catchup=False, schedule_interval='@daily') as dag:

    start_test = EmptyOperator(
        task_id="start_test"
    )

    end_test = EmptyOperator(
        task_id="end_test"
    )

    down_load_task = download_task()

    collect_number_crawled_date = PythonOperator(
        task_id = 'collect_number_crawled_date',
        python_callable=_check_quantity
    )


    # check_log_in = check_log_in_available()

    # start_test >> check_log_in >> down_load_task >> end_test

    start_test >> down_load_task >> collect_number_crawled_date >>end_test