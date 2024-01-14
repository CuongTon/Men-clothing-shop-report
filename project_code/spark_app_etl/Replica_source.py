import shutil
from datetime import datetime
import sys
sys.path.append('/home/cuongton/airflow/')
from project_setting import generall_setting

if __name__ == "__main__":
    now = datetime.now()
    year = now.year
    month = now.month
    day = now.day

    source = f"/home/cuongton/airflow/project_code/crawl_shopee_data/{generall_setting.folder_name_staging_layer}/{year}/{month}/{day}"
    destination = f"/home/cuongton/airflow/project_code/crawl_shopee_data/{generall_setting.folder_name_staging_layer}_replica/{year}/{month}/{day}"

    # create or overwrite folder
    # remove a directory if it exists
    shutil.rmtree(destination, ignore_errors=True)
    # copy
    shutil.copytree(source, destination)