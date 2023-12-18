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


from datetime import datetime

current_date = datetime.today()

print(current_date.day, current_date.month, current_date.year)