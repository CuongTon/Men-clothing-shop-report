from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
 
from datetime import datetime
 
with DAG('test', start_date=datetime(2023, 12, 18), 
    schedule_interval='@daily', catchup=False) as dag:
 
    start = EmptyOperator(
        task_id = 'start'
    )

    end = EmptyOperator(
        task_id = 'end',
        trigger_rule='none_failed'
    )

    extract_a = BashOperator(
        task_id='extract_a',
        bash_command="""
            source ~/airflow/bin/activate
            python3 /home/cuongton/airflow/selenium_multiple_profile/profile_1.py
        """
    )
 
    extract_b = BashOperator(
        task_id='extract_b',
        bash_command="""
            source ~/airflow/bin/activate
            python3 /home/cuongton/airflow/selenium_multiple_profile/profile_2.py
        """
    )

    extract_c = BashOperator(
        task_id='extract_c',
        bash_command="""
            source ~/airflow/bin/activate
            python3 /home/cuongton/airflow/selenium_multiple_profile/profile_3.py
        """
    )
 
    start >> [extract_a, extract_b, extract_c] >> end