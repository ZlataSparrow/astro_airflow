from airflow.sdk import dag, task, chain
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

    @task
    def task_e():
        print("Hello from task_e")

    # Create task instances ONCE
    a = task_a()
    b = task_b()
    c = task_c()
    d = task_d()
    e = task_e()

    # Set dependencies so the graph is:
    # A → B → C
    # A → D → E
    a >> b >> c
    a >> d >> e

my_dag()