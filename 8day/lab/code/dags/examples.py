from datetime import datetime

import requests
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


def print_start():
    print("Starting Airflow DAG")


def print_date():
    print(f"Current date and time: {datetime.now()}")


with DAG(
    "simple_dag",
    start_date=datetime(2024, 9, 18),
    schedule_interval="@daily",
    catchup=False,
) as dag1:
    start_task = PythonOperator(task_id="print_start", python_callable=print_start)
    date_task = PythonOperator(task_id="print_date", python_callable=print_date)

    start_task >> date_task


def print_hello():
    print("Hello from Airflow")


with DAG(
    "operators_dag",
    start_date=datetime(2024, 9, 18),
    schedule_interval="*/5 * * * *",
    catchup=False,
) as dag2:
    bash_task = BashOperator(
        task_id="bash_task", bash_command='echo "Running Bash task"'
    )

    python_task = PythonOperator(task_id="python_task", python_callable=print_hello)

    bash_task >> python_task


# Task to push data into XCom
def push_data_to_xcom(**context):
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    context["ti"].xcom_push(
        key="current_time", value=f"{current_time} from python function"
    )


# DAG definition
with DAG(
    "xcom_example_dag",
    start_date=datetime(2024, 9, 18),
    schedule_interval=None,
    catchup=False,
) as dag3:

    # PythonOperator to push data
    push_task = PythonOperator(
        task_id="push_data", python_callable=push_data_to_xcom, provide_context=True
    )

    # BashOperator to pull data and echo it
    pull_task = BashOperator(
        task_id="pull_data",
        bash_command='echo "The current time is {{ ti.xcom_pull(task_ids="push_data", key="current_time") }}"',
    )

    push_task >> pull_task
