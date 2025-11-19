from airflow.sdk import dag, DAG, task
from airflow.providers.standard.operators.python import PythonOperator
from pendulum import datetime

def task_aa():
    print("Hello from task_a")

@dag(
    dag_id="my_dag2",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    description="This dags does something...",
    tags=["team_a","source_a"],
    max_consecutive_failed_dag_runs=3,
) 


def my_dag2():

    @task
    def task_aa():
        print("Hello from task_a")
    task_aa()

my_dag2()