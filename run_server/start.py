import subprocess 
import time

subprocess.run(["airflow", "webserver", "--daemon"], check=True)
time.sleep(1)
subprocess.run(["airflow", "scheduler", "--daemon"], check=True)
time.sleep(1)
subprocess.run(["airflow", "celery", "worker",  "--daemon"], check=True)
time.sleep(1)
subprocess.run(["airflow", "celery", "flower",  "--daemon"], check=True)