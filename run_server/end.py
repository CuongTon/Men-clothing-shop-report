import subprocess 

try:
    subprocess.run(["pkill", "-f", "airflow webserver"], check=True)
    print('airflow webserver shut down')
except:
    pass

try:
    subprocess.run(["pkill", "-f", "airflow scheduler"], check=True)
    print('airflow scheduler shut down')
except:
    pass

try:
    subprocess.run(["pkill", "-f", "celery worker"], check=True)
    print('airflow celery worker shut down')
except:
    pass

try:
    subprocess.run(["pkill", "-f", "celery flower"], check=True)
    print('airflow celery flower shut down')
except:
    pass
