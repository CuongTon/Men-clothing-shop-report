import subprocess 

subprocess.run(["pkill", "-f", "airflow webserver"], check=True)
subprocess.run(["pkill", "-f", "airflow scheduler"], check=True)
subprocess.run(["pkill", "-f", "celery worker"], check=True)
subprocess.run(["pkill", "-f", "celery flower"], check=True)