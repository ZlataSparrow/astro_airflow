from airflow.sdk import dag, DAG, task

from pendulum import datetime

# with DAG
@dag(
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    description="This dags does something...",
    tags=["team_a","source_a"],
    max_consecutive_failed_dag_runs=3,
) #:


def my_dag():
    
    @task
    def task_a():
        print("Hello")
    task_a()


my_dag()