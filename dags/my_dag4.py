from airflow.sdk import dag, DAG, task
from airflow.providers.standard.operators.python import PythonOperator
from pendulum import datetime

def my_dag():

    @task
    def task_a():
        print("Hello from task_a")
    task_a()

    @task
    def task_b():
        print("Hello from task_b")
    task_b()

    @task
    def task_c():
        print("Hello from task_c")
    task_c()

    @task
    def task_d():
        print("Hello from task_d")
    task_d()

    task_a() >> task_b() >> task_c() >> task_d()

my_dag()