from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
import sys
sys.path.append('/home/cuongton/airflow/')
from project_code.data_mart_etl.daily_sale_star_schema import ETL_daily_sale_data_mart

def ETL_daily_sale():

    with TaskGroup("ETL_daily_sale", tooltip="ETL daily sale data mart") as group:
        
        product_dim = PythonOperator(
            task_id = "product_dim",
            python_callable=ETL_daily_sale_data_mart().get_product,
            trigger_rule = 'none_failed'
        )

        rating_dim = PythonOperator(
            task_id = "rating_dim",
            python_callable=ETL_daily_sale_data_mart().get_rating,
            trigger_rule = 'none_failed'
        )

        discount_dim = PythonOperator(
            task_id = "discount_dim",
            python_callable=ETL_daily_sale_data_mart().get_discount,
            trigger_rule = 'none_failed'
        )

        shop_dim = PythonOperator(
            task_id = "shop_dim",
            python_callable=ETL_daily_sale_data_mart().get_shop,
            trigger_rule = 'none_failed'
        )

        time_dim = PythonOperator(
            task_id = "time_dim",
            python_callable=ETL_daily_sale_data_mart().get_time,
            trigger_rule = 'none_failed'
        )

        daily_sale_fact = PythonOperator(
            task_id = "daily_sale_fact",
            python_callable=ETL_daily_sale_data_mart().get_daily_revenue,
        )

        [product_dim, rating_dim, discount_dim, shop_dim, time_dim] >> daily_sale_fact

        return group