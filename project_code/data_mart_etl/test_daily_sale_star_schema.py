from pymongo import MongoClient, ReplaceOne
from datetime import datetime, date, timedelta
import math
import logging
import sys
sys.path.append('/home/cuongton/airflow/')
from project_setting import time_setting, generall_setting

class ETL_daily_sale_data_mart:

    # set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # set up database, collections and other information.
    def __init__(self):

        # default is 1 day
        self.delay_time = generall_setting.delay_time_for_first_day_run_ETL

        # Connect to MongoDB
        self.client = MongoClient("mongodb://192.168.1.20:27017")

        # Access the MongoDB database and collection
        self.db = self.client[generall_setting.Mongo_Database] #testing. Change back to ShopeeVN_airflow
        self.men_shop_collection = self.db[generall_setting.Mongo_Collection]
        self.first_day = time_setting.start_time # it will delay one 1 day when reading data from MongoDB. While Spark reads the exact day, pymongo read a one-day delay      

    # Function to create product_detail_dim collection.
    def get_product(self):

        bulk = []
        documents = self.men_shop_collection.find()

        for doc in documents:
            item = {
                "itemid": doc["itemid"],
                "name": doc["name"],
                "stock": doc["stock"],
                "historical_sold": doc["historical_sold"],
                "price_before_discount": doc["price_before_discount"],
                "images_url": doc["images_url"],
                "create_time": doc["create_time"]
            }
            bulk.append(ReplaceOne(item, item, upsert=True))

        before_counter = self.db["product_detail_dim"].count_documents({})

        if len((bulk)):
            self.db["product_detail_dim"].bulk_write(bulk)
            counter = self.db["product_detail_dim"].count_documents({}) - before_counter
            logging.info(f'Inserted {counter} items into product_detail_dim')
            return True
        else:
            logging.warning("No new item insert into product_detail_dim")

    # Function to create rating_dim collection.
    def get_rating(self):
        bulk = []
        documents = self.men_shop_collection.find()

        for doc in documents:
            item = {
                "rating_star": doc["rating_star"],
                "total_vote": doc["total_vote"],
                "five_stars": doc["five_stars"],
                "four_stars": doc["four_stars"],
                "three_stars": doc["three_stars"],
                "two_stars": doc["two_stars"],
                "one_star": doc["one_star"],
                "liked_count": doc["liked_count"],
                "cmt_count": doc["cmt_count"]
            }
            bulk.append(ReplaceOne(item, item, upsert=True))

        before_counter = self.db["rating_dim"].count_documents({})

        if len((bulk)):
            self.db["rating_dim"].bulk_write(bulk)
            counter = self.db["rating_dim"].count_documents({}) - before_counter
            logging.info(f'Inserted {counter} items into rating_dim')
            return True
        else:
            logging.warning("No new item insert into rating_dim")

    # Function to create discount_dim collection.
    def get_discount(self):
        bulk = []
        documents = self.men_shop_collection.find()

        for doc in documents:
            item = {
                "discount_rate": doc["discount"]
            }
            bulk.append(ReplaceOne(item, item, upsert=True))
        
        before_counter = self.db["discount_dim"].count_documents({})

        if len((bulk)):
            self.db["discount_dim"].bulk_write(bulk)
            counter = self.db["discount_dim"].count_documents({}) - before_counter
            logging.info(f'Inserted {counter} items into discount_dim')
            return True
        else:
            logging.warning("No new item insert into discount_dim")

    # Function to create shop_dim collection.
    def get_shop(self):
        bulk = []
        documents = self.men_shop_collection.find()

        for doc in documents:
            item = {
                "shopid": doc["shopid"],
                "shop_name": doc["shop_name"],
                "shop_rating": doc["shop_rating"]
            }
            bulk.append(ReplaceOne(item, item, upsert=True))
        
        before_counter = self.db["shop_dim"].count_documents({})

        if len((bulk)):
            self.db["shop_dim"].bulk_write(bulk)
            counter = self.db["shop_dim"].count_documents({}) - before_counter
            logging.info(f'Inserted {counter} items into shop_dim')
            return True
        else:
            logging.warning("No new item insert into shop_dim")


    # Function to create time_dim collection.
    def get_time(self):
        bulk = []

        current_date = datetime.now()-timedelta(days=self.delay_time)
        current_date_without_hour = current_date.strftime("%Y-%m-%d")
        current_day = current_date.day
        current_week = current_date.isocalendar()[1]
        current_month = current_date.month    
        current_quarter = math.ceil(current_date.month / 3)
        current_year = current_date.year

        item = {
            "date": current_date_without_hour,
            "day": current_day,
            "week": current_week,
            "month": current_month,
            "quarter": current_quarter,
            "year": current_year
        }

        bulk.append(ReplaceOne(item, item, upsert=True))

        before_counter = self.db["time_dim"].count_documents({})

        if len((bulk)):
            self.db["time_dim"].bulk_write(bulk)
            counter = self.db["time_dim"].count_documents({}) - before_counter
            logging.info(f'Inserted {counter} items into time_dim')
            return True
        else:
            logging.warning("No new item insert into time_dim")

    # Function to create daily_sale_fact collection.
    def get_daily_revenue(self):
        bulk = []
        documents = self.men_shop_collection.find()
        
        for doc in documents:
            # find new document which is inserted today and the status is current
            if doc['start_date'].date() == date.today() - timedelta(days=self.delay_time) and doc['current_flag'] == 'Current':
                # set up quantity and price unit
                quantity_sold = 0
                current_price = 0
                # find the expried items, have expiration date is today
                expired_item = self.men_shop_collection.find_one({'itemid': doc['itemid'], 'current_flag': 'Expired', 
                                                        'expiration_date': doc['start_date']}) # this will return None or a dictionary
                if expired_item is None: 
                    # there is three cases when an expired item of pervious date is None:
                    # First case: first time run ETL, so all the items are new items and ignore historical_sold
                    # Second case: newly items add today
                    # Third case: there is an item which is deleted before, and now add into shop again
                    if doc['start_date'].date() == self.first_day.date(): # and deleted_item_before is None: # first ETL / First case
                        quantity_sold = 0

                    else: 
                        # launch a new item or add items which were deleted before
                        cursor_expired_items = self.men_shop_collection.find({'itemid': doc['itemid'], 'current_flag': 'Expired'}) # this will return a cursor, use list() to turn it into python list class
                        lst_expried_items = list(cursor_expired_items)

                        if len(lst_expried_items): # Third case
                            lastest_expried_items = sorted(lst_expried_items, key= lambda x: x['expiration_date'], reverse=True)[0]
                            quantity_sold = doc['historical_sold'] - lastest_expried_items['historical_sold']
                            current_price = doc['current_price']                       
                        else: # Second case
                            quantity_sold = doc['historical_sold']
                            current_price = doc['current_price']

                else:
                    # so we have a previous expired items, just take current date - previous date to get quantity different
                    quantity_sold = doc['historical_sold'] - expired_item['historical_sold']
                    current_price = expired_item['current_price']
                    doc = expired_item

                revenue = quantity_sold * current_price

                if revenue: # insert new line when there is a new revenue
                    item = {
                        'quantity': quantity_sold,
                        'price_unit': current_price,
                        'revenue': revenue
                    }

                    # reference other documents
                    product_detail = self.db['product_detail_dim'].find_one({
                        "itemid": doc["itemid"],
                        "name": doc["name"],
                        "stock": doc["stock"],
                        "historical_sold": doc["historical_sold"],
                        "price_before_discount": doc["price_before_discount"],
                        "images_url": doc["images_url"],
                        "create_time": doc["create_time"]
                    })

                    rating = self.db['rating_dim'].find_one({
                        "rating_star": doc["rating_star"],
                        "total_vote": doc["total_vote"],
                        "five_stars": doc["five_stars"],
                        "four_stars": doc["four_stars"],
                        "three_stars": doc["three_stars"],
                        "two_stars": doc["two_stars"],
                        "one_star": doc["one_star"],
                        "liked_count": doc["liked_count"],
                        "cmt_count": doc["cmt_count"]
                    })

                    discount = self.db['discount_dim'].find_one({
                        "discount_rate": doc["discount"]
                    })

                    shop_info = self.db['shop_dim'].find_one({
                        "shopid": doc["shopid"],
                        "shop_name": doc["shop_name"],
                        "shop_rating": doc["shop_rating"]
                    })

                    current_date = datetime.now()-timedelta(days=self.delay_time)
                    current_date_without_hour = current_date.strftime("%Y-%m-%d")

                    time = self.db.time_dim.find_one({
                        "date": current_date_without_hour
                    })
                    
                    item['product_detail_id'] = product_detail['_id']
                    item['rating_id'] = rating['_id']
                    item['discount_id'] = discount['_id']
                    item['time_id'] = time['_id']
                    item['shop_info_id'] = shop_info['_id']

                    bulk.append(ReplaceOne(item, item, upsert=True))

                else:
                    continue
                
        before_counter = self.db["daily_sale_fact"].count_documents({})

        if len((bulk)):
            self.db["daily_sale_fact"].bulk_write(bulk)
            counter = self.db["daily_sale_fact"].count_documents({}) - before_counter
            logging.info(f'Inserted {counter} items into daily_sale_fact')
            return True
        else:
            logging.warning("No new item insert into daily_sale_fact")



if __name__ == '__main__':

    # default is 1 day
    delay_time = generall_setting.delay_time_for_first_day_run_ETL

    # Connect to MongoDB
    client = MongoClient("mongodb://192.168.1.20:27017")

    # Access the MongoDB database and collection
    db = client[generall_setting.Mongo_Database] #testing. Change back to ShopeeVN_airflow
    men_shop_collection = db[generall_setting.Mongo_Collection]
    first_day = time_setting.start_time # it will delay one 1 day when read data from MongoDB. Spark read an exact day, but pymongo read 1 day delay

    now = datetime.now()-timedelta(days=delay_time) 
    start_of_day = datetime(now.year, now.month, now.day, 0, 0, 0, 0)
    print(start_of_day)
    items = db['MenClothingShop_airflow'].find({'start_date': {'$gt': start_of_day}})
    
    for i in items:
        print(i['start_date'])

    # # insert new items
    # ETL_daily_sale_data_mart().get_product()
    # ETL_daily_sale_data_mart().get_rating()
    # ETL_daily_sale_data_mart().get_discount()
    # ETL_daily_sale_data_mart().get_shop()
    # ETL_daily_sale_data_mart().get_time()
    # ETL_daily_sale_data_mart().get_daily_revenue()
