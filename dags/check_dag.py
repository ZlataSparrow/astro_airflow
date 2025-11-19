from airflow.sdk import dag, task
from pendulum import datetime 

@dag(
    dag_id="check_dag",
    schedule="0 0 * * *",             # midnight every day
    start_date=datetime(2025, 1, 1),
    description="DAG to check data",
    tags=["data_engineering"],
)
def my_dag():

    @task.bash(task_id="create_file")
    def create_file():
        return 'echo "Hi there!" > /tmp/dummy'

    @task.bash(task_id="check_file")
    def check_file():
        return 'test -f /tmp/dummy'

    @task(task_id="read_file")
    def read_file():
        print(open('/tmp/dummy', 'rb').read())

    # Create task instances
    create = create_file()
    check = check_file()
    read = read_file()

    # Set dependencies so the graph is:
    create >> check >> read


my_dag()