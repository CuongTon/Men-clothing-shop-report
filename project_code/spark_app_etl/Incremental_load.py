from pyspark.sql import SparkSession
import secret

def fetch_data_S3(spark, S3_path):
    # read data from S3
    raw_data = spark.read \
        .format('json') \
        .option('multiline', 'true') \
        .load(S3_path)
    # tranform data
    data = raw_data.selectExpr('itemid', "shopid", 'shop_name', 'name', 'stock', "historical_sold",
                               'price/100000 as current_price', "price_min/100000 as current_price_min", "price_max/100000 as current_price_max",
                               "case when price_before_discount != 0 then price_before_discount/100000 else price/100000 end as price_before_discount",
                               "case when price_min_before_discount = -1 then price_min/100000 else price_min_before_discount/100000 end as price_min_before_discount",
                               "case when price_max_before_discount = -1 then price_max/100000 else price_max_before_discount/100000 end as price_max_before_discount",
                               "cast(1-raw_discount/100 as decimal(5,2)) as discount", "cast(from_unixtime(ctime, 'yyyy-MM-dd') as date) as create_time",
                               'item_rating.rating_star as rating_star', 'item_rating.rating_count[0] as total_vote', 'item_rating.rating_count[5] as five_stars',
                               'item_rating.rating_count[4] as four_stars', 'item_rating.rating_count[3] as three_stars', 'item_rating.rating_count[2] as two_stars',
                               'item_rating.rating_count[1] as one_star', "liked_count", "cmt_count","shop_rating", "concat('https://down-vn.img.susercontent.com/file/', image) as images_url",
                               "'Current' as n_current_flag", "current_date() as n_start_date", "date('2999-12-31') as n_expiration_date"
                               )
    return data

def retrieve_data_from_MongoDB(spark, database, collection):
    raw_data = spark.read \
        .format('com.mongodb.spark.sql.DefaultSource') \
        .option('uri', f'mongodb://192.168.1.20/{database}.{collection}') \
        .load()

    current_data = raw_data.where("current_flag = 'Current'")

    return current_data

def load_data_to_MongoDB(tranformed_data, mode, database, collection):
    tranformed_data.write \
        .format("com.mongodb.spark.sql.DefaultSource") \
        .option('uri', f'mongodb://192.168.1.20/{database}.{collection}') \
        .mode(mode) \
        .save()

def update_new_data(new_data, old_data):
    # condiiton to join 2 data frames
    join_condition = ['itemid', "shopid", 'shop_name', 'name', 'stock', "historical_sold", 'current_price', 'current_price_min', 'current_price_max',
                      'price_before_discount', 'price_min_before_discount', 'price_max_before_discount', 'discount', 'create_time', 'rating_star','total_vote',
                      'five_stars', 'four_stars', 'three_stars', 'two_stars', 'one_star', 'liked_count', 'cmt_count', "shop_rating", 'images_url'
                      ]

    outer_join_data = new_data.join(old_data, join_condition, 'fullouter') \
        .where("_id is null or n_start_date is null")

    if new_data.count() != 0:

        update_data = outer_join_data.selectExpr('itemid', "shopid", 'shop_name', 'name', 'stock', "historical_sold", 'current_price', 'current_price_min', 'current_price_max',
                    'price_before_discount', 'price_min_before_discount', 'price_max_before_discount', 'discount', 'create_time', 'rating_star', 'total_vote',
                    'five_stars', 'four_stars', 'three_stars', 'two_stars', 'one_star', 'liked_count', 'cmt_count', "shop_rating", 'images_url',
                    '_id', "case when n_current_flag is null then 'Expired' else 'Current' end as current_flag",
                    "case when n_start_date is null then start_date else n_start_date end as start_date",
                    "case when n_expiration_date is null then current_date() else n_expiration_date end as expiration_date"
                    )
        return update_data
    else:
        return False


if __name__ == '__main__':

    # create spark entry point
    spark = SparkSession.builder \
        .master('local') \
        .appName('Spark S3') \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.1,org.apache.hadoop:hadoop-common:3.3.1,org.mongodb.spark:mongo-spark-connector_2.12:3.0.2') \
        .getOrCreate()

    # set up spark to connect S3
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", secret.Access_Key)
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret.Secret_Key)

    # set up log
    logger = spark.sparkContext._jvm.org.apache.log4j.LogManager.getLogger(__name__)

    # read latest data from S3
    S3_Path = 's3a://shopeeproject/ShopeeShop/MenClothingShop.json'
    latest_KraftVN = fetch_data_S3(spark, S3_Path)

    # read current data from MongoDB
    database = 'ShopeeVN_airflow' #changelater
    collection = 'MenClothingShop_airflow' #changelater
    current_Shop = retrieve_data_from_MongoDB(spark, database,collection)

    # find a new change data
    update_Shop = update_new_data(latest_KraftVN, current_Shop)

    # load data to MongoDB
    if update_Shop:
        # summary SCD
        total_input_items = update_Shop.count()
        expired_items = update_Shop.where("_id is not null").count()
        updated_items = update_Shop.where("_id is null").count()
        new_items = updated_items - expired_items

        load_data_to_MongoDB(update_Shop, 'append', database, collection)
        logger.warn(f'{total_input_items} items are updated, in which: new items: {new_items}; updated current items: {updated_items}; expired items: {expired_items}.')
    else:
        logger.warn("No new data")
