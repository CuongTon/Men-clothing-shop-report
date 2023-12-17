import json
import boto3
import secret

json_list = ['4men.json', 'biluxury.json', 'Coolmate.json', 'highway.json', 'justmen.json', 'Kraftvn.json', 'levents.json', 'routine.json', 'ssstutter.json', 'yame.json']

merged_data = []

for file in json_list:
    with open(f'/home/cuongton/airflow/project_code/crawl_shopee_data/Raw_Data/{file}', 'r', encoding='utf-8') as json_file:
        merged_data = merged_data + (json.loads(json_file.read()))

s3 = boto3.client('s3', aws_access_key_id=secret.Access_Key, aws_secret_access_key=secret.Secret_Key)
s3.put_object(
     Body=json.dumps(merged_data),
     Bucket='shopeeproject',
     Key='Kraftvn/myfinal_data.json'
)
