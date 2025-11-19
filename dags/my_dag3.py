from airflow.sdk import dag, DAG, task
from airflow.providers.standard.operators.python import PythonOperator
from pendulum import datetime

default_args = {
    'retries': 3,
}

@dag('my_dag3', start_date=datetime(2025, 1 , 1), default_args=default_args,
         description='A simple tutorial DAG', tags=['data_science'],
         schedule='@daily')
def my_dag3():
    
    task_a = PythonOperator(task_id='task_a', python_callable=print_a)
    task_b = PythonOperator(task_id='task_b', python_callable=print_b)
    task_c = PythonOperator(task_id='task_c', python_callable=print_c)
    task_d = PythonOperator(task_id='task_d', python_callable=print_d)
    task_e = PythonOperator(task_id='task_e', python_callable=print_e)

    chain(task_a, [task_b, task_c], [task_d, task_e])