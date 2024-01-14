from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
import sys
sys.path.append('/home/cuongton/airflow/')
from project_setting import shop_setting


def check_log_in_available():

    with TaskGroup("check_log_in_available", tooltip="Check if log in is available") as group:
        
        for i in shop_setting.profile_list:
            
            task = BashOperator(
                task_id = f'{i}_Profile',
                bash_command=f"python3 /home/cuongton/airflow/project_code/crawl_shopee_data/crawl_shopee_data/Check_Log_In.py {i}_Profile"
            )

        return group