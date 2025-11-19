"""
Example showing BashOperator with TaskFlow API (using @task.bash decorator).

In Airflow 2.5+, you can use @task.bash decorator for bash commands with TaskFlow API.
"""

from airflow.sdk import dag, task
from pendulum import datetime


@dag(
    dag_id="bash_taskflow_example",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    description="Example using @task.bash decorator",
    tags=["example", "bash", "taskflow"],
    catchup=False,
)
def bash_taskflow_example():
    """
    Using @task.bash decorator to create files with TaskFlow API.
    """

    @task.bash
    def create_file_task():
        """
        Create a file using bash command.
        The function should return a bash command string.
        """
        return 'echo "Hello from TaskFlow Bash!" > /tmp/taskflow_output.txt'

    @task.bash
    def create_directory_and_file():
        """
        Create a directory and then a file inside it.
        """
        return """
        mkdir -p /tmp/airflow_data && \
        echo "Data file created at $(date)" > /tmp/airflow_data/data.txt && \
        echo "Directory and file created successfully"
        """

    @task.bash
    def append_to_file():
        """
        Append content to an existing file.
        """
        return 'echo "This line was appended" >> /tmp/taskflow_output.txt'

    @task.bash
    def list_files():
        """
        List files and show their contents.
        """
        return """
        echo "=== Files in /tmp ===" && \
        ls -lh /tmp/*.txt 2>/dev/null || echo "No .txt files found" && \
        echo "" && \
        echo "=== Contents of taskflow_output.txt ===" && \
        cat /tmp/taskflow_output.txt 2>/dev/null || echo "File not found"
        """

    # Set dependencies
    create_file_task() >> create_directory_and_file() >> append_to_file() >> list_files()


# Instantiate the DAG
bash_taskflow_example()

