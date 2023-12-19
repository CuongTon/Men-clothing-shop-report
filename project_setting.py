from datetime import datetime

class time_setting():
   start_time = datetime(2023, 12, 17, 9, 0)

class shop_setting():   
   shop_list = ['4MEN', 'Biluxury', 'Coolmate', 'Highway', 'Justmen', 'Kraftvn', 'Levents', 'Routine', 'SSSTUTTER', 'YaMe']

class generall_setting():
   Mongo_Database = 'ShopeeVN_airflow' #testing. Change it back to ShopeeVN_airflow
   Mongo_Collection = 'MenClothingShop_airflow' 
   delay_time_for_rerun_S3 = 0 # adjust time for Merge_and_Put_s3, initial and incremental ETL. Default is 0
   delay_time_for_first_day_run_ETL = 1 # adjust time for daily_sale_star_schema. Default is 1

   # set up S3 link
   S3_Bucket = 'shopeeproject'
   S3_Key = 'ShopeeShop/MenClothingShop.json'
   S3_path = f's3a://{S3_Bucket}/{S3_Key}'