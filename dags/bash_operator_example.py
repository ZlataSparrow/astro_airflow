"""
Example DAG showing how to use BashOperator to create files.

BashOperator allows you to execute bash/shell commands in Airflow tasks.
This is useful for file operations, running scripts, system commands, etc.
"""

from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator
from pendulum import datetime


@dag(
    dag_id="bash_operator_file_example",
    schedule=None,  # Manual trigger only
    start_date=datetime(2025, 1, 1),
    description="Example of using BashOperator to create files",
    tags=["example", "bash"],
    catchup=False,
)
def bash_file_example():
    """
    This DAG demonstrates different ways to create files using BashOperator.
    """

    # Method 1: Using echo to create a simple text file
    create_simple_file = BashOperator(
        task_id="create_simple_file",
        bash_command='echo "Hello from Airflow!" > /tmp/airflow_output.txt',
    )

    # Method 2: Create a file with multiple lines using heredoc
    create_multiline_file = BashOperator(
        task_id="create_multiline_file",
        bash_command="""
        cat > /tmp/multiline_output.txt << 'EOF'
        Line 1: This is the first line
        Line 2: This is the second line
        Line 3: This is the third line
        Created by: Airflow BashOperator
        EOF
        """,
    )

    # Method 3: Create a file with dynamic content using Airflow context
    # Note: In BashOperator, you can use Jinja templating
    create_dynamic_file = BashOperator(
        task_id="create_dynamic_file",
        bash_command="""
        cat > /tmp/dynamic_output.txt << EOF
        DAG Run ID: {{ dag_run.run_id }}
        Execution Date: {{ ds }}
        Task Instance: {{ ti.task_id }}
        Timestamp: $(date)
        EOF
        """,
    )

    # Method 4: Create a JSON file
    create_json_file = BashOperator(
        task_id="create_json_file",
        bash_command="""
        cat > /tmp/data.json << 'EOF'
        {
            "name": "Airflow Example",
            "type": "BashOperator",
            "created_by": "Airflow",
            "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
        }
        EOF
        """,
    )

    # Method 5: Create a file and verify it was created
    verify_file = BashOperator(
        task_id="verify_file",
        bash_command="""
        if [ -f /tmp/airflow_output.txt ]; then
            echo "File created successfully!"
            echo "File contents:"
            cat /tmp/airflow_output.txt
        else
            echo "File not found!"
            exit 1
        fi
        """,
    )

    # Set task dependencies
    create_simple_file >> create_multiline_file >> create_dynamic_file >> create_json_file >> verify_file


# Instantiate the DAG
bash_file_example()

