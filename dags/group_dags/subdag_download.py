from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
import sys
sys.path.append('/home/cuongton/airflow/')
from project_setting import shop_setting, generall_setting
from datetime import datetime, timedelta
from project_code.crawl_shopee_data.crawl_shopee_data.spiders import Master

def download_task():

    with TaskGroup("downloads", tooltip="Crawling Part") as group:
        
        #save raw data by date
        current_date = datetime.today()

        shop_dict = shop_setting.shop_details
        tasks_list = []
      
        for num, shop in enumerate(shop_dict):
            #create task instance
            task = PythonOperator(
                task_id = f'download_{shop["name"]}',
                python_callable=Master.main,
                op_kwargs= {
                    'json_path': f"/home/cuongton/airflow/project_code/crawl_shopee_data/{generall_setting.folder_name_staging_layer}/{current_date.year}/{current_date.month}/{current_date.day}/{shop['name']}.json",
                    'shop_name': shop["name"],
                    'shop_id':shop['shop_id'],
                    'shopee_url': shop['google_search_shop_link_first_page'],
                    'profile':'/home/cuongton/airflow/project_code/crawl_shopee_data/' + shop['chrome_profile_path']
                },
                retries=3,
                retry_delay=timedelta(seconds=10)
            )

            # task = BashOperator(
            #     task_id = f'download_{shop["name"]}',
            #     bash_command=f'''
            #         source ~/airflow/bin/activate
            #         cd /home/cuongton/airflow/project_code/crawl_shopee_data
            #         scrapy crawl Master -a para_1={shop['name']} -a para_2={shop['shop_id']} -a para_3={shop['google_search_shop_link_first_page']} -a para_4={shop['chrome_profile_path']} -O {generall_setting.folder_name_staging_layer}/{current_date.year}/{current_date.month}/{current_date.day}/{shop['name']}.json
            #     ''',
            #     retries=3,
            #     retry_delay=timedelta(seconds=10)
            # )

            tasks_list.append(task)

            if num % 2 == 1:
                tasks_list[0] >> tasks_list[1]
                tasks_list = []

        return group
    
# ['4MEN', 'Biluxury', 'Coolmate', 'Highway', 'Justmen', 'Kraftvn', 'Levents', 'Routine', 'SSSTUTTER', 'YaMe']