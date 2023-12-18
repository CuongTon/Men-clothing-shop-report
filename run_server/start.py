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
