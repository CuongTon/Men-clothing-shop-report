from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
import sys
sys.path.append('/home/cuongton/airflow/')
from project_setting import shop_setting
from datetime import datetime


def download_task():

    with TaskGroup("downloads", tooltip="Crawling Part") as group:
        
        #save raw data by date
        current_date = datetime.today()

        shop_list = shop_setting.shop_list
        tasks_even = []
        tasks_odd = []
        for num, shop in enumerate(shop_list):
            task = BashOperator(
                task_id = f'download_{shop}',
                bash_command=f'''
                    source ~/airflow/bin/activate
                    cd /home/cuongton/airflow/project_code/crawl_shopee_data
                    scrapy crawl {shop} -O RawData/{current_date.year}/{current_date.month}/{current_date.day}/{shop}.json
                '''
            )
            if num % 2 == 0:
                tasks_even.append(task)
            else:
                tasks_odd.append(task)

        for num in range(len(tasks_even)-1):
            tasks_even[num] >> tasks_even[num+1]  

        for num in range(len(tasks_odd)-1):
            tasks_odd[num] >> tasks_odd[num+1]  

        return group
    
# ['4MEN', 'Biluxury', 'Coolmate', 'Highway', 'Justmen', 'Kraftvn', 'Levents', 'Routine', 'SSSTUTTER', 'YaMe']