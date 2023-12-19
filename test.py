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


from datetime import datetime, date

# current_date = datetime.today()

# print(current_date.day, current_date.month, current_date.year, current_date.date(), date.today())


dct = [{'item_id': 1, 'issue_date': datetime(2023,12,19)}, {'item_id': 3, 'issue_date': datetime(2023,12,17)}, {'item_id': 2, 'issue_date': datetime(2023,12,10)}]

new_dct = sorted(dct, key=lambda x: x['issue_date'],reverse=True)[0]

print(new_dct)

mylist = []

print(mylist is None)


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


S3_Bucket = 'shopeeproject'
S3_Key = 'ShopeeShop/MenClothingShop.json'
S3_path = f's3a://{S3_Bucket}/{S3_Key}'

print(S3_path)