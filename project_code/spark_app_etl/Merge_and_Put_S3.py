import json
import boto3
import secret
import sys
sys.path.append('/home/cuongton/airflow/')
from project_setting import shop_setting
from datetime import datetime


def combine_json_file(list_of_json_file):

    current_date = datetime.today()

    merged_data = []
    for file in list_of_json_file:
        with open(f'/home/cuongton/airflow/project_code/crawl_shopee_data/RawData/{current_date.year}/{current_date.month}/{current_date.day}/{file}.json', 'r', encoding='utf-8') as json_file:
            merged_data = merged_data + (json.loads(json_file.read()))
    return merged_data

if __name__ == '__main__':

    json_list = shop_setting.shop_list
    final_data = combine_json_file(json_list)

    s3 = boto3.client('s3', aws_access_key_id=secret.Access_Key, aws_secret_access_key=secret.Secret_Key)
    s3.put_object(
        Body=json.dumps(final_data),
        Bucket='shopeeproject',
        Key='ShopeeShop/MenClothingShop.json'
    )

# ['4MEN.json', 'Biluxury.json', 'Coolmate.json', 'Highway.json', 'Justmen.json', 'Kraftvn.json', 'Levents.json', 'Routine.json', 'SSSTUTTER.json', 'YaMe.json']