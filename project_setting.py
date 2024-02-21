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

   # Shop Detail
   # Default: refer to shop_infor.json
   # Affection: subdag_download, daily_sale_dashboard, Merge_and_Put_S3
   shop_details = [
    {"shop_id": 277366270,
     "name": "4MEN",
     "google_search_shop_link_first_page": "https://shopee.vn/4menstores#product_list",
     "chrome_profile_path": "crawl_shopee_data/ChromeProfile/default_First_Profile"
     },
     {"shop_id": 68988783,
     "name": "Biluxury",
     "google_search_shop_link_first_page": "https://shopee.vn/thoitrangbiluxury#product_list",
     "chrome_profile_path": "crawl_shopee_data/ChromeProfile/default_First_Profile"
     },
     {"shop_id": 24710134,
     "name": "Coolmate",
     "google_search_shop_link_first_page": "coolmate.vn#shopee",
     "chrome_profile_path": "crawl_shopee_data/ChromeProfile/default_Second_Profile"
     },
     {"shop_id": 413372243,
     "name": "Highway",
     "google_search_shop_link_first_page": "https://shopee.vn/highwaymenswear#product_list",
     "chrome_profile_path": "crawl_shopee_data/ChromeProfile/default_Second_Profile"
     },
     {"shop_id": 129448137,
     "name": "Justmen",
     "google_search_shop_link_first_page": "https://shopee.vn/justmen_officialstore#product_list",
     "chrome_profile_path": "crawl_shopee_data/ChromeProfile/default_Third_Profile"
     },
     {"shop_id": 623651329,
     "name": "Kraftvn",
     "google_search_shop_link_first_page": "https://shopee.vn/kraftvn#product_list",
     "chrome_profile_path": "crawl_shopee_data/ChromeProfile/default_Third_Profile"
     },
     {"shop_id": 317477677,
     "name": "Levents",
     "google_search_shop_link_first_page": "https://shopee.vn/levents.vn#product_list",
     "chrome_profile_path": "crawl_shopee_data/ChromeProfile/default_Fourth_Profile"
     },
     {"shop_id": 210001661,
     "name": "Routine",
     "google_search_shop_link_first_page": "https://shopee.vn/routinevn?page=0",
     "chrome_profile_path": "crawl_shopee_data/ChromeProfile/default_Fourth_Profile"
     },
     {"shop_id": 179042207,
     "name": "SSSTUTTER",
     "google_search_shop_link_first_page": "https://shopee.vn/ssstutter#product_list",
     "chrome_profile_path": "crawl_shopee_data/ChromeProfile/default_Fifth_Profile"
     },
     {"shop_id": 473918762,
     "name": "YaMe",
     "google_search_shop_link_first_page": "https://shopee.vn/yame_vn",
     "chrome_profile_path": "crawl_shopee_data/ChromeProfile/default_Fifth_Profile"
     }
]


'''
[
    {"shop_id": 277366270,
     "name": "4MEN",
     "google_search_shop_link_first_page": "https://shopee.vn/4menstores#product_list",
     "chrome_profile_path": "crawl_shopee_data/ChromeProfile/default_First_Profile"
     },
     {"shop_id": 68988783,
     "name": "Biluxury",
     "google_search_shop_link_first_page": "https://shopee.vn/thoitrangbiluxury#product_list",
     "chrome_profile_path": "crawl_shopee_data/ChromeProfile/default_First_Profile"
     },
     {"shop_id": 24710134,
     "name": "Coolmate",
     "google_search_shop_link_first_page": "https://shopee.vn/coolmate.vn",
     "chrome_profile_path": "crawl_shopee_data/ChromeProfile/default_Second_Profile"
     },
     {"shop_id": 413372243,
     "name": "Highway",
     "google_search_shop_link_first_page": "https://shopee.vn/highwaymenswear#product_list",
     "chrome_profile_path": "crawl_shopee_data/ChromeProfile/default_Second_Profile"
     },
     {"shop_id": 129448137,
     "name": "Justmen",
     "google_search_shop_link_first_page": "https://shopee.vn/justmen_officialstore#product_list",
     "chrome_profile_path": "crawl_shopee_data/ChromeProfile/default_Third_Profile"
     },
     {"shop_id": 623651329,
     "name": "Kraftvn",
     "google_search_shop_link_first_page": "https://shopee.vn/kraftvn#product_list",
     "chrome_profile_path": "crawl_shopee_data/ChromeProfile/default_Third_Profile"
     },
     {"shop_id": 317477677,
     "name": "Levents",
     "google_search_shop_link_first_page": "https://shopee.vn/levents.vn#product_list",
     "chrome_profile_path": "crawl_shopee_data/ChromeProfile/default_Fourth_Profile"
     },
     {"shop_id": 210001661,
     "name": "Routine",
     "google_search_shop_link_first_page": "https://shopee.vn/routinevn?page=0",
     "chrome_profile_path": "crawl_shopee_data/ChromeProfile/default_Fourth_Profile"
     },
     {"shop_id": 179042207,
     "name": "SSSTUTTER",
     "google_search_shop_link_first_page": "https://shopee.vn/ssstutter#product_list",
     "chrome_profile_path": "crawl_shopee_data/ChromeProfile/default_Fifth_Profile"
     },
     {"shop_id": 473918762,
     "name": "YaMe",
     "google_search_shop_link_first_page": "https://shopee.vn/yame_vn",
     "chrome_profile_path": "crawl_shopee_data/ChromeProfile/default_Fifth_Profile"
     }
]

'''

class generall_setting():
   # Default: ShopeeVN_airflow
   Mongo_Database = 'ShopeeVN_airflow_test' #TBU
   Mongo_Collection = 'MenClothingShop_airflow' 

   # Default: 0
   # Affection: adjust time for Merge_and_Put_s3, initial and incremental ETL. 
   delay_time_for_rerun_S3 = 0 

   # Default: 1
   # Affection: adjust time for daily_sale_star_schema
   delay_time_for_first_day_run_ETL = 1

   # set up S3 link
   S3_Bucket = 'shopeeproject'
   S3_Key = 'ShopeeShop/MenClothingShop.json'
   S3_path = f's3a://{S3_Bucket}/{S3_Key}'

   # Folder_name_staging_layer: 
   # Affection: group_dags - download tasks, Merge_and_Put_S3, Replica_source
   # Default: RawData
   folder_name_staging_layer = "RawData_1" #TBU