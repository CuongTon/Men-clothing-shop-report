from pymongo import MongoClient, ReplaceOne
from datetime import datetime, date, timedelta
import math
import logging
import sys
sys.path.append('/home/cuongton/airflow/')
from project_setting import time_setting

# set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Function to create product_detail_dim collection
def get_product():

    bulk = []
    documents = men_shop_collection.find()

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

    before_counter = db["product_detail_dim"].count_documents({})

    if len((bulk)):
        db["product_detail_dim"].bulk_write(bulk)
        counter = db["product_detail_dim"].count_documents({}) - before_counter
        logging.info(f'Inserted {counter} items into product_detail_dim')
        return True
    else:
        logging.warning("No new item insert into product_detail_dim")

# Function to create rating_dim collection
def get_rating():
    bulk = []
    documents = men_shop_collection.find()

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

    before_counter = db["rating_dim"].count_documents({})

    if len((bulk)):
        db["rating_dim"].bulk_write(bulk)
        counter = db["rating_dim"].count_documents({}) - before_counter
        logging.info(f'Inserted {counter} items into rating_dim')
        return True
    else:
        logging.warning("No new item insert into rating_dim")

# Function to create discount_dim collection
def get_discount():
    bulk = []
    documents = men_shop_collection.find()

    for doc in documents:
        item = {
            "discount_rate": doc["discount"]
        }
        bulk.append(ReplaceOne(item, item, upsert=True))
    
    before_counter = db["discount_dim"].count_documents({})

    if len((bulk)):
        db["discount_dim"].bulk_write(bulk)
        counter = db["discount_dim"].count_documents({}) - before_counter
        logging.info(f'Inserted {counter} items into discount_dim')
        return True
    else:
        logging.warning("No new item insert into discount_dim")

# Function to create shop_dim collection
def get_shop():
    bulk = []
    documents = men_shop_collection.find()

    for doc in documents:
        item = {
            "shopid": doc["shopid"],
            "shop_name": doc["shop_name"],
            "shop_rating": doc["shop_rating"]
        }
        bulk.append(ReplaceOne(item, item, upsert=True))
    
    before_counter = db["shop_dim"].count_documents({})

    if len((bulk)):
        db["shop_dim"].bulk_write(bulk)
        counter = db["shop_dim"].count_documents({}) - before_counter
        logging.info(f'Inserted {counter} items into shop_dim')
        return True
    else:
        logging.warning("No new item insert into shop_dim")


# Function to create time_dim collection
def get_time():
    bulk = []

    current_date = datetime.now()-timedelta(days=delay_time)
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

    before_counter = db["time_dim"].count_documents({})

    if len((bulk)):
        db["time_dim"].bulk_write(bulk)
        counter = db["time_dim"].count_documents({}) - before_counter
        logging.info(f'Inserted {counter} items into time_dim')
        return True
    else:
        logging.warning("No new item insert into time_dim")

# Function to create daily_sale_fact collection
def get_daily_revenue():
    bulk = []
    documents = men_shop_collection.find()
    
    for doc in documents:
        # find new document which is inserted today and the status is current
        if doc['start_date'].date() == date.today() - timedelta(days=delay_time) and doc['current_flag'] == 'Current':
            # set up quantity and price unit
            quantity_sold = 0
            current_price = 0
            # find the expried items, have expiration date is today
            expired_item = men_shop_collection.find_one({'itemid': doc['itemid'], 'current_flag': 'Expired', 
                                                    'expiration_date': doc['start_date']})
            
            if expired_item is None: 
                # there is two cases when an expired item is None, so we just have new item and don't expired item. First ETL or they launch a new product.
                if doc['start_date'].date() == first_day.date(): # first ETL
                    quantity_sold = 0
                else: # launch a new item
                    quantity_sold = doc['historical_sold']
                    current_price = doc['current_price']
            else: 
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
                product_detail = db['product_detail_dim'].find_one({
                    "itemid": doc["itemid"],
                    "name": doc["name"],
                    "stock": doc["stock"],
                    "historical_sold": doc["historical_sold"],
                    "price_before_discount": doc["price_before_discount"],
                    "images_url": doc["images_url"],
                })

                rating = db['rating_dim'].find_one({
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

                discount = db['discount_dim'].find_one({
                    "discount_rate": doc["discount"]
                })

                current_date = datetime.now()-timedelta(days=delay_time)
                current_date_without_hour = current_date.strftime("%Y-%m-%d")

                time = db.time_dim.find_one({
                    "date": current_date_without_hour
                })
                
                item['product_detail_id'] = product_detail['_id']
                item['rating_id'] = rating['_id']
                item['discount_id'] = discount['_id']
                item['time_id'] = time['_id']

                bulk.append(ReplaceOne(item, item, upsert=True))

            else:
                continue
            
    before_counter = db["daily_sale_fact"].count_documents({})

    if len((bulk)):
        db["daily_sale_fact"].bulk_write(bulk)
        counter = db["daily_sale_fact"].count_documents({}) - before_counter
        logging.info(f'Inserted {counter} items into daily_sale_fact')
        return True
    else:
        logging.warning("No new item insert into daily_sale_fact")



if __name__ == '__main__':

    # default is 1 day
    delay_time = 1

    # Connect to MongoDB
    client = MongoClient("mongodb://192.168.1.20:27017")

    # Access the MongoDB database and collection
    db = client["ShopeeVN_airflow"]
    men_shop_collection = db["MenClothingShop_airflow"]
    first_day = time_setting.start_time # it will delay one 1 day when read data from MongoDB. Spark read an exact day, but pymongo read 1 day delay

    # insert new items
    get_product()
    get_rating()
    get_discount()
    get_shop()
    get_time()
    get_daily_revenue()
