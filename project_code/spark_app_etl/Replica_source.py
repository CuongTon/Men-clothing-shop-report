import shutil
from datetime import datetime


if __name__ == "__main__":
    now = datetime.now()
    year = now.year
    month = now.month
    day = now.day

    source = f"/home/cuongton/airflow/project_code/crawl_shopee_data/RawData/{year}/{month}/{day}"
    destination = f"/home/cuongton/airflow/project_code/crawl_shopee_data/RawData_replica/{year}/{month}/{day}"

    # create or overwrite folder
    # remove a directory if it exists
    shutil.rmtree(destination, ignore_errors=True)
    # copy
    shutil.copytree(source, destination)