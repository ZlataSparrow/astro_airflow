from airflow.sdk import dag, DAG, task
from airflow.providers.standard.operators.python import PythonOperator
from pendulum import datetime

def_task_a():
    print("Hello from task_a")
# with DAG
@dag(
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    description="This dags does something...",
    tags=["team_a","source_a"],
    max_consecutive_failed_dag_runs=3,
) #:


def my_dag():
    
#    @task
#    def task_a():
#    task_a = PythonOperator(
#        task_id="task_a",
#        python_callable=task_a,
#    )
#    print("Hello")
#    task_a()

    task_a = PythonOperator(
        task_id="task_a",
        python_callable=task_a,
    )


my_dag()