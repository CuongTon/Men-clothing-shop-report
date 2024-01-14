from datetime import datetime

class time_setting():
   start_time = datetime(2023, 12, 17, 9, 0)

class shop_setting():   

   # Default: ['4MEN', 'Biluxury', 'Coolmate', 'Highway', 'Justmen', 'Kraftvn', 'Levents', 'Routine', 'SSSTUTTER', 'YaMe']
   # Note: Keep a number of shops is even
   shop_list = ['4MEN', 'Biluxury', 'Coolmate', 'Highway', 'Justmen', 'Kraftvn', 'Levents', 'Routine', 'SSSTUTTER', 'YaMe']

   # Test case: ['test_First', 'test_Second', 'test_Third', 'test_Fourth', 'test_Fifth']
   # Default: ['default_First', 'default_Second', 'default_Third', 'default_Fourth', 'default_Fifth']
   profile_list = ['default_First', 'default_Second', 'default_Third', 'default_Fourth', 'default_Fifth']


class generall_setting():
   # Default: ShopeeVN_airflow
   Mongo_Database = 'ShopeeVN_airflow'
   Mongo_Collection = 'MenClothingShop_airflow' 

   # Default: 0
   # Affect: adjust time for Merge_and_Put_s3, initial and incremental ETL. 
   delay_time_for_rerun_S3 = 0 # 

   # Default: 1
   # Affect: adjust time for daily_sale_star_schema
   delay_time_for_first_day_run_ETL = 1

   # set up S3 link
   S3_Bucket = 'shopeeproject'
   S3_Key = 'ShopeeShop/MenClothingShop.json'
   S3_path = f's3a://{S3_Bucket}/{S3_Key}'

   # Folder_name_staging_layer: affect group_dags and Merge_and_Put_S3
   # Default: RawData
   folder_name_staging_layer = "RawData"