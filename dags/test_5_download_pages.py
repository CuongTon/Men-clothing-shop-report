from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from group_dags.subdag_5_profiles_test import download_task
from group_dags.check_5_profile_log_in_test import check_log_in_available

with DAG("test_5_download_pages", start_date=datetime(2023, 12, 31), catchup=False, schedule_interval='@daily') as dag:

    start_test = EmptyOperator(
        task_id="start_test"
    )

    end_test = EmptyOperator(
        task_id="end_test"
    )

    down_load_task = download_task()

    check_log_in = check_log_in_available()

    start_test >> check_log_in >> down_load_task >> end_test