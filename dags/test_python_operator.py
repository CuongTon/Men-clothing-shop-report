from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import sys
# sys.path.append('/home/cuongton/airflow')
# from project_code.crawl_shopee_data.crawl_shopee_data.spiders import Master_Python_Operator, Test_Log, Test


with DAG('test_python_operator', catchup=False, start_date=datetime(2024, 1, 28), schedule_interval='@daily') as dag:

    start = EmptyOperator(
        task_id='start'
    )
    
    end = EmptyOperator(
        task_id='end'
    )

    # small_python = PythonOperator (
    #     task_id = 'small_python',
    #     python_callable=Test.main,
    #     op_kwargs={
    #         'name': 'Peter',
    #         'amount': 100000
    #     }
    # )

    # test_python = PythonOperator (
    #     task_id = 'test_python',
    #     python_callable=Test_Log.main
    # )

    # test_Master = PythonOperator (
    #     task_id = 'test_Master',
    #     python_callable=Master_Python_Operator.main,
    #     op_kwargs = {
    #         'json_path': '/home/cuongton/airflow/project_code/crawl_shopee_data/Yame.json',
    #         'shop_name': 'YaMe',
    #         'shop_id': 473918762,
    #         'shopee_url': "https://shopee.vn/yame_vn",
    #         'profile': '/home/cuongton/airflow/project_code/crawl_shopee_data/'+"crawl_shopee_data/ChromeProfile/default_Fifth_Profile"
    #     }
    # )

    # start >> small_python >> test_python >> end


    # test_python = PythonOperator (
    #     task_id = 'test_python',
    #     python_callable=Master_Python_Operator.main,
    #     op_kwargs = {
    #         'json_path': '/home/cuongton/airflow/project_code/crawl_shopee_data/Yame.json',
    #         'shop_name': 'YaMe',
    #         'shop_id': 473918762,
    #         'shopee_url': "https://shopee.vn/yame_vn",
    #         'profile': '/home/cuongton/airflow/project_code/crawl_shopee_data/'+"crawl_shopee_data/ChromeProfile/default_Fifth_Profile"
    #     }     
    # )