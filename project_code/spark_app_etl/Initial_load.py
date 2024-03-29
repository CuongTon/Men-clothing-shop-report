from pyspark.sql import SparkSession
import secret
import sys
sys.path.append('/home/cuongton/airflow/')
from project_setting import generall_setting
from datetime import date, timedelta

def fetch_data_S3(spark, S3_path):
    # read data from S3
    raw_data = spark.read \
        .format('json') \
        .option('multiline', 'true') \
        .load(S3_path) # testing. Change it back S3_path when finish testing
    # tranform data
    data = raw_data.selectExpr('itemid', "shopid", 'shop_name', 'name', 'stock', "historical_sold",
                               'price/100000 as current_price', "price_min/100000 as current_price_min", "price_max/100000 as current_price_max",
                               "case when price_before_discount != 0 then price_before_discount/100000 else price/100000 end as price_before_discount",
                               "case when price_min_before_discount = -1 then price_min/100000 else price_min_before_discount/100000 end as price_min_before_discount",
                               "case when price_max_before_discount = -1 then price_max/100000 else price_max_before_discount/100000 end as price_max_before_discount",
                               "cast(1-raw_discount/100 as decimal(5,2)) as discount", "cast(from_unixtime(ctime, 'yyyy-MM-dd') as date) as create_time",
                               'item_rating.rating_star as rating_star', 'item_rating.rating_count[0] as total_vote', 'item_rating.rating_count[5] as five_stars',
                               'item_rating.rating_count[4] as four_stars', 'item_rating.rating_count[3] as three_stars', 'item_rating.rating_count[2] as two_stars',
                               'item_rating.rating_count[1] as one_star', "liked_count", "cmt_count", "shop_rating", "concat('https://down-vn.img.susercontent.com/file/', image) as images_url",
                               "'Current' as current_flag", f"date('{current_date}') as start_date", "date('2999-12-31') as expiration_date"
                               )
    return data

def load_data_to_MongoDB(tranformed_data, mode, database, collection):
    tranformed_data.write \
        .format("com.mongodb.spark.sql.DefaultSource") \
        .option('uri', f'mongodb://192.168.1.20/{database}.{collection}') \
        .mode(mode) \
        .save()

if __name__ == '__main__':
    # create spark entry point
    spark = SparkSession.builder \
        .master('local') \
        .appName('Spark S3') \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.1,org.apache.hadoop:hadoop-common:3.3.1,org.mongodb.spark:mongo-spark-connector_2.12:3.0.2') \
        .getOrCreate()
    logger = spark.sparkContext._jvm.org.apache.log4j.LogManager.getLogger(__name__)
    # set up spark to connect S3
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", secret.Access_Key)
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret.Secret_Key)

    # set current date. Default timedelta is 0
    current_date = date.today() - timedelta(days=generall_setting.delay_time_for_rerun_S3)

    # read data from S3
    S3_Path = generall_setting.S3_path
    MenClothing = fetch_data_S3(spark, S3_Path)

    # load data to MongoDB
    total_items = MenClothing.count()
    database = generall_setting.Mongo_Database 
    collection = generall_setting.Mongo_Collection 
    load_data_to_MongoDB(MenClothing, 'overwrite', database, collection)
    logger.warn(f'Successfully load {total_items} into MongoDB Warehouse')