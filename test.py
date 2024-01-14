# from pymongo import MongoClient 


# server = MongoClient("mongodb://192.168.1.20:27017")
# db = server['ShopeeVN_airflow']
# collection = db['KraftVN_airflow_1']
# if collection.find_one():
#     print('incremental_load')
# else:
#     print('initial_load')


# import subprocess 

# subprocess.run(["airflow", "webserver", "--daemon"], check=True)
# subprocess.run(["airflow", "scheduler", "--daemon"], check=True)
# subprocess.run(["airflow", "celery", "worker",  "--daemon"], check=True)
# subprocess.run(["airflow", "celery", "flower",  "--daemon"], check=True)

# from datetime import datetime

# time = 1626165776
# print(datetime.fromtimestamp(time))


# shop = 'KraftVN'

# mystring = f'''
#     source ~/airflow/bin/activate
#     cd /home/cuongton/airflow/project_code/crawl_shopee_data
#     scrapy crawl {shop} -O {shop}.json
# '''

# print(mystring)


# import scrapy
# import undetected_chromedriver as webdriver
# from selenium.webdriver.common.by import By
# from selenium.webdriver.common.keys import Keys
# import time
# import json


    
#general information
# name = "4MEN"
# shop_id = 277366270
# google_search_shop_link_first_page = 'https://shopee.vn/4menstores#product_list'

# distract scrapy
# S3 file configuration
# custom_settings = {
#     'FEEDS':{f's3://shopeeproject/Kraftvn/{name}.json': {
#         'format': 'json',
#         'encoding': 'utf8',
#         'store_empty': False,
#         'indent': 4}                
#     }
# }


# set up selenium
# chrome_options = webdriver.ChromeOptions()
# chrome_options.add_argument("--user-data-dir=project_code/crawl_shopee_data/crawl_shopee_data/kraft_profile_v2")
# driver = webdriver.Chrome(options=chrome_options)
# # Enable network tracking

# # navigate to a destinated website
# driver.get('https://www.google.com')

# search_input = driver.find_element(By.XPATH, '//textarea[@type="search"]')
# search_input.send_keys(google_search_shop_link_first_page)
# search_input.send_keys(Keys.ENTER)

# first_page = driver.find_element(By.XPATH, '(//h3[@class="LC20lb MBeuO DKV0Md"])[1]')
# first_page.click()

# time.sleep(5)


# from datetime import datetime, date

# current_date = datetime.today()

# print(current_date.day, current_date.month, current_date.year, current_date.date(), date.today())


# dct = [{'item_id': 1, 'issue_date': datetime(2023,12,19)}, {'item_id': 3, 'issue_date': datetime(2023,12,17)}, {'item_id': 2, 'issue_date': datetime(2023,12,10)}]

# new_dct = sorted(dct, key=lambda x: x['issue_date'],reverse=True)[0]

# print(new_dct)

# mylist = []

# print(mylist is None)


# function delete_dim_table(databaseName) {
#     const delete_dim_list = ["discount_dim", "product_detail_dim", "rating_dim", "shop_dim", "time_dim", "daily_sale_fact"];


#     const db = db.getSiblingDB(databaseName);

#     delete_dim_list.forEach(function (collection) {
#         if (db.getCollectionNames().indexOf(collection) >= 0) {
#             db.getCollection(collection).drop();
#             print("Deleted Collection: " + collection);
#         }
#     });
# }

# delete_dim_list.forEach( function (collection) { if (db.getCollectionNames().indexOf(collection)>=0) { db.getCollection(collection).drop(); print("Deleted Collection: "+ collection); } })


# S3_Bucket = 'shopeeproject'
# S3_Key = 'ShopeeShop/MenClothingShop.json'
# S3_path = f's3a://{S3_Bucket}/{S3_Key}'

# print(S3_path)


# from airflow import DAG
# from airflow.operators.bash import BashOperator
# from airflow.utils.task_group import TaskGroup
# import sys
# sys.path.append('/home/cuongton/airflow/')
# from project_setting import shop_setting, generall_setting
# from datetime import datetime              

# current_date = datetime.today()
# shop_list = shop_setting.shop_list


# for shop in shop_list:
#     bash_command=f'''
#     source ~/airflow/bin/activate
#     cd /home/cuongton/airflow/project_code/crawl_shopee_data
#     scrapy crawl {shop} -O {generall_setting.folder_name_staging_layer}/{current_date.year}/{current_date.month}/{current_date.day}/{shop}.json
# '''
#     print(bash_command)    


'''
   source ~/airflow/bin/activate
    cd /home/cuongton/airflow/project_code/crawl_shopee_data
    scrapy crawl 4MEN -O RawData/2023/12/29/4MEN.json
    done

    source ~/airflow/bin/activate
    cd /home/cuongton/airflow/project_code/crawl_shopee_data
    scrapy crawl Biluxury -O RawData/2023/12/29/Biluxury.json
    done

    source ~/airflow/bin/activate
    cd /home/cuongton/airflow/project_code/crawl_shopee_data
    scrapy crawl Coolmate -O RawData/2023/12/29/Coolmate.json
    done

    source ~/airflow/bin/activate
    cd /home/cuongton/airflow/project_code/crawl_shopee_data
    scrapy crawl Highway -O RawData/2023/12/29/Highway.json
    done

    source ~/airflow/bin/activate
    cd /home/cuongton/airflow/project_code/crawl_shopee_data
    scrapy crawl Justmen -O RawData/2023/12/29/Justmen.json
    done

    source ~/airflow/bin/activate
    cd /home/cuongton/airflow/project_code/crawl_shopee_data
    scrapy crawl Kraftvn -O RawData/2023/12/29/Kraftvn.json
    done

    source ~/airflow/bin/activate
    cd /home/cuongton/airflow/project_code/crawl_shopee_data
    scrapy crawl Levents -O RawData/2023/12/29/Levents.json
    done

    source ~/airflow/bin/activate
    cd /home/cuongton/airflow/project_code/crawl_shopee_data
    scrapy crawl Routine -O RawData/2023/12/29/Routine.json
    done

    source ~/airflow/bin/activate
    cd /home/cuongton/airflow/project_code/crawl_shopee_data
    scrapy crawl SSSTUTTER -O RawData/2023/12/29/SSSTUTTER.json
    done

    source ~/airflow/bin/activate
    cd /home/cuongton/airflow/project_code/crawl_shopee_data
    scrapy crawl YaMe -O RawData/2023/12/29/YaMe.json
    
'''

# import json

# with open('/home/cuongton/airflow/project_code/crawl_shopee_data/RawData_1/2024/1/14/Kraftvn.json', 'r', encoding='utf-8') as file:
#     lst = json.loads(file.read())
#     dct = json.dumps(lst, indent=1)
#     print(dct)

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

for index, shop in enumerate(shop_details):
    print(f"scrapy crawl Master -a para_1={shop['name']} -a para_2={shop['shop_id']} -a para_3={shop['google_search_shop_link_first_page']} -a para_4={shop['chrome_profile_path']}")