from airflow.sdk import dag, task
from pendulum import datetime

@dag(
    dag_id="my_dag4",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    description="A simple DAG with task dependencies",
    tags=["example"],
)
def my_dag():

    @task
    def task_a():
        print("Hello from task_a")

    @task
    def task_b():
        print("Hello from task_b")

    @task
    def task_c():
        print("Hello from task_c")

    @task
    def task_d():
        print("Hello from task_d")

    # Set task dependencies
    task_a() >> task_b() >> task_c() >> task_d()

my_dag()