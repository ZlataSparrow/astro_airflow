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
    def task_a(): ...

    @task
    def task_b(): ...

    @task
    def task_c(): ...

    @task
    def task_d(): ...

    @task
    def task_e(): ...

    a = task_a()
    b = task_b()
    c = task_c()
    d = task_d()
    e = task_e()

    # either with chain:
    chain(a, [b, d], [c, e])

    # or explicitly:
    # a >> [b, d]
    # b >> c
    # d >> e

my_dag()