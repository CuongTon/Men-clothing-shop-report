import subprocess

commands = [
    "airflow webserver -p 8080 &",
    "airflow scheduler &",
    "airflow celery worker &",
    "airflow celery flower"
]

# Run each command independently
for command in commands:
    subprocess.run(command, shell=True)

#8793
#sudo fuser -k 8793/tcp #This will kill all process associated with port 8793