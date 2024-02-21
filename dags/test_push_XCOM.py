from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime 
import sys
sys.path.append('/home/cuongton/airflow')
from selenium_multiple_profile.Modual_task import randomClass, _check_function
import logging

def my_func(**context):
    # Create a custom logger
    logger = logging.getLogger('my_custom_logger')

    # Set the log level
    logger.setLevel(logging.ERROR)

    # Create a console handler
    handler = logging.StreamHandler()
    handler.setLevel(logging.ERROR)

    # Create a formatter and add it to the handler
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(handler)

    # Log some messages
    logger.info('This is an info message')
    logger.warning('This is a warning message')
    logger.error('This is an error message')

with DAG('test_push_XCOM', start_date=datetime(2024,1, 21), catchup=False, schedule_interval='@daily') as dag:

    start = EmptyOperator(
        task_id = 'start'
    )

    test_XCOM = PythonOperator(
        task_id = 'test_Xcom',
        python_callable=randomClass()._test_XCOM
    )

    kwags_XCOM = PythonOperator(
        task_id = 'kwags_Xcom',
        python_callable=_check_function
    )

    # bash_XCOM = BashOperator(
    #     task_id = 'bashXCOM',
    #     bash_command='''

    #     source ~/airflow/bin/activate
    #     python3 /home/cuongton/airflow/selenium_multiple_profile/Modual_task.py
    #     output=20
    #     echo$ouput
    #     echo "{{ ti.xcom_push(key='my_key', value=output) }}"
    # '''
    # )
#        echo '{{ ti.xcom_push(key='my_key_python', value='my_value') }}'
    
    log_task = PythonOperator(
    task_id='log_task',
    python_callable=my_func,
    provide_context=True,
    dag=dag
    )


    end = EmptyOperator(
        task_id = 'end'
    )

    start >> test_XCOM >> kwags_XCOM >> log_task >> end