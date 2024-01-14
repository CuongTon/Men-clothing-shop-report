from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
import sys
sys.path.append('/home/cuongton/airflow/')
from project_setting import shop_setting, generall_setting
from datetime import datetime


def download_task():

    with TaskGroup("downloads", tooltip="Crawling Part") as group:
        
        #save raw data by date
        current_date = datetime.today()

        shop_dict = shop_setting.shop_details
        tasks_even = []
      
        for num, shop in enumerate(shop_dict):
            task = BashOperator(
                task_id = f'download_{shop["name"]}',
                bash_command=f'''
                    source ~/airflow/bin/activate
                    cd /home/cuongton/airflow/project_code/crawl_shopee_data
                    scrapy crawl Master -a para_1={shop['name']} -a para_2={shop['shop_id']} -a para_3={shop['google_search_shop_link_first_page']} -a para_4={shop['chrome_profile_path']} -O {generall_setting.folder_name_staging_layer}/{current_date.year}/{current_date.month}/{current_date.day}/{shop['name']}.json
                '''
            )
# scrapy crawl {shop} -O {generall_setting.folder_name_staging_layer}/{current_date.year}/{current_date.month}/{current_date.day}/{shop}.json
            tasks_even.append(task)

            if num % 2 == 1:
                tasks_even[0] >> tasks_even[1]
                tasks_even = []

        return group
    
# ['4MEN', 'Biluxury', 'Coolmate', 'Highway', 'Justmen', 'Kraftvn', 'Levents', 'Routine', 'SSSTUTTER', 'YaMe']