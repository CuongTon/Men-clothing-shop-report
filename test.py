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

from datetime import datetime

time = 1626165776
print(datetime.fromtimestamp(time))