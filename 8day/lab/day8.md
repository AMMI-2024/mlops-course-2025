# Lab: Orchestration with Airflow and GitHub Actions

## Table of Contents

- [Lab: Orchestration with Airflow and GitHub Actions](#lab-orchestration-with-airflow-and-github-actions)
  - [Table of Contents](#table-of-contents)
  - [Theory Overview](#theory-overview)
    - [Orchestration in Machine Learning Pipelines](#orchestration-in-machine-learning-pipelines)
    - [Key Concepts](#key-concepts)
  - [Part 1: Introduction to Airflow](#part-1-introduction-to-airflow)
    - [Task 1: Create a Simple Airflow DAG](#task-1-create-a-simple-airflow-dag)
    - [Task 2: Using Airflow Operators](#task-2-using-airflow-operators)
    - [Task 3: Running a DAG every 5 minutes](#task-3-running-a-dag-every-5-minutes)
    - [Task 4: Using XCom for Task Communication](#task-4-using-xcom-for-task-communication)
      - [What is `ti` in Airflow?](#what-is-ti-in-airflow)
      - [XCom Task Example](#xcom-task-example)
  - [Part 2: Fetch, Process, Save](#part-2-fetch-process-save)
    - [Steps](#steps)
  - [Part 3: GitHub Workflow Integration](#part-3-github-workflow-integration)
    - [Task 1: GitHub Actions Workflow (`.github/workflows/print_temperature.yml`)](#task-1-github-actions-workflow-githubworkflowsprint_temperatureyml)
    - [Task 2: Generate GitHub Token with Write Permission](#task-2-generate-github-token-with-write-permission)
    - [Task 3: Airflow Task to Trigger GitHub Workflow](#task-3-airflow-task-to-trigger-github-workflow)
  - [Conclusion](#conclusion)
  - [Useful Links](#useful-links)

---

## Theory Overview

### Orchestration in Machine Learning Pipelines

Orchestration is the process of automating the execution of tasks in a pipeline. In machine learning, this often involves scheduling tasks like data fetching, data preprocessing, model training, evaluation, and deployment. Orchestration tools like **Apache Airflow** make it easier to manage complex workflows, ensuring that each step runs smoothly and at the correct time.

**Airflow** uses DAGs (Directed Acyclic Graphs) to represent workflows. Each node in the graph is a task, and the edges between them define dependencies.

### Key Concepts

- **DAG**: A Directed Acyclic Graph, which represents the entire workflow or pipeline.
- **Task**: A single step in the pipeline. Tasks can perform actions like fetching data, running scripts, or sending notifications.
- **Operator**: Defines what type of task is being performed (e.g., a Python function, a bash command, etc.).
- **GitHub Actions**: A CI/CD automation tool integrated with GitHub. It is event-driven and can be triggered by events such as code pushes, pull requests, or even API calls. It allows workflows to run custom scripts or pre-built actions.

---

## Part 1: Introduction to Airflow

### Task 1: Create a Simple Airflow DAG

In this task, you'll create a simple DAG that runs two Python tasks in sequence.

```python
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def print_start():
    print("Starting Airflow DAG")

def print_date():
    print(f"Current date and time: {datetime.now()}")

with DAG('simple_dag', start_date=datetime(2023, 1, 1), schedule_interval='@daily', catchup=False) as dag1:
    start_task = PythonOperator(
        task_id='print_start',
        python_callable=print_start
    )
    date_task = PythonOperator(
        task_id='print_date',
        python_callable=print_date
    )

    start_task >> date_task
```

### Task 2: Using Airflow Operators

Create a DAG using the `BashOperator` and `PythonOperator`.

```python
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def print_hello():
    print("Hello from Airflow")

with DAG('operators_dag', start_date=datetime(2024, 9, 18), schedule_interval=None, catchup=False) as dag2:
    bash_task = BashOperator(
        task_id='bash_task',
        bash_command='echo "Running Bash task"'
    )

    python_task = PythonOperator(
        task_id='python_task',
        python_callable=print_hello
    )

    bash_task >> python_task
```

### Task 3: Running a DAG every 5 minutes

In this task, you’ll modify your DAG to run every 5 minutes.

To achieve this, modify the `schedule_interval` in your DAG definition:

```python
with DAG('operators_dag', start_date=datetime(2024, 9, 18), schedule_interval='*/5 * * * *', catchup=False) as dag2:
```

This cron expression (`*/5 * * * *`) runs the DAG every 5 minutes.

**Note:** You can use [crontab.guru](https://crontab.guru/) to quickly figure out a cron schedule expression you might need.

### Task 4: Using XCom for Task Communication

In this task, you'll learn how to use XCom (Cross-Communication) to pass data between tasks. We will use both a `PythonOperator` and a `BashOperator`.

#### What is `ti` in Airflow?

In Airflow, `ti` stands for **TaskInstance**. It is an object that represents a single run of a task. When using `XCom`, we often refer to `ti` because it allows us to **push** and **pull** values (such as variables) between tasks. For example, you can push a value using `ti.xcom_push()` and retrieve it in another task with `ti.xcom_pull()`.

#### XCom Task Example

1. **PythonOperator**: Push a value (e.g., the current timestamp) using `XCom`.
2. **BashOperator**: Pull the value using `XCom` and echo it in the Bash command.

```python
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.utils.dates import days_ago

# Task to push data into XCom
def push_data_to_xcom(**context):
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    context['ti'].xcom_push(key='current_time', value=f"{current_time} from python function")

# DAG definition
with DAG('xcom_example_dag', start_date=datetime(2024, 9, 18), schedule_interval=None, catchup=False) as dag3:
    
    # PythonOperator to push data
    push_task = PythonOperator(
        task_id='push_data',
        python_callable=push_data_to_xcom,
        provide_context=True
    )
    
    # BashOperator to pull data and echo it
    pull_task = BashOperator(
        task_id='pull_data',
        bash_command='echo "The current time is {{ ti.xcom_pull(task_ids='push_data', key='current_time') }}"'
    )

    push_task >> pull_task
```

---

## Part 2: Fetch, Process, Save

In this part, you'll fetch weather data from an API, process it, save it to a local file.

### Steps

1. **Create an account** for the [Visual Crossing](https://www.visualcrossing.com/) and get your API key.
2. **Fetch** weather data using the VisualCrossing API and save the temperature to XCom. You can use any location you want, for example London.
3. **Process** the temperature in a separate task/function to convert it to a different unit (i.e., Fahrenheit, Kelvin) and save it again to XCom. This is to demonstrate that you know how to work with XCom.
4. **Save** the processed data to a local file.

```python
import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from google.cloud import storage

# Fetch weather data
def fetch_weather_data(**context):
    api_key = "YOUR_API_KEY"
    city = "London"
    response = requests.get(
        f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{city}?unitGroup=metric&include=current&key={api_key}&contentType=json"
    )
    data = response.json()
    temp = data["currentConditions"]["temp"]
    context["ti"].xcom_push(key="temperature", value=temp)

# Process data
def process_data(**context):
    temperature = context["ti"].xcom_pull(task_ids="fetch_weather_data", key="temperature")
    temp_k = temperature + 273.15
    context['ti'].xcom_push(key="temp_K", value=temp_k)

def save_to_file(**context):
    temp_k = context['ti'].xcom_pull(task_ids='process_data', key="temp_K")

    with open('./tmp/weather_data.txt', 'w') as f:
        f.write(f"Current temperature: {temp_c} °K")

# DAG definition
with DAG('weather_pipeline', start_date=datetime(2023, 1, 1), schedule_interval='@daily', catchup=False) as dag:
    fetch_task = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_weather_data,
        provide_context=True
    )

    process_task = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
        provide_context=True
    )

    save_task = PythonOperator(
        task_id='save_data',
        python_callable=save_to_file,
        provide_context=True
    )

    fetch_task >> process_task >> save_task
```

---

## Part 3: GitHub Workflow Integration

In this part, you'll write a simple GitHub Actions workflow and trigger it from Airflow.

### Task 1: GitHub Actions Workflow (`.github/workflows/print_temperature.yml`)

In this task, you’ll create a simple GitHub Actions workflow that will echo the temperature passed from Airflow.

```yaml
name: Print Temperature
on:
  workflow_dispatch:
    inputs:
      temperature:
        description: 'The temperature to print'
        required: true
jobs:
  print_temperature:
    runs-on: ubuntu-latest
    steps:
      - name: Echo the temperature
        run: echo "The temperature is ${{ github.event.inputs.temperature }} degrees"
```

### Task 2: Generate GitHub Token with Write Permission

- Go to your GitHub account settings under **Developer Settings > Personal Access Tokens**.
- Generate a new fine-grained token with **write access** to `actions` in your repository.

### Task 3: Airflow Task to Trigger GitHub Workflow

Now that your workflow is ready, trigger it from Airflow using the temperature fetched from the weather API.

```python
import requests

def trigger_github_workflow(**context):
    temperature = context["ti"].xcom_pull(task_ids="process_data", key="temp_K")
    github_token = 'your_github_token'
    headers = {
        'Authorization': f'token {github_token}',
        'Accept': 'application/vnd.github.v3+json'
    }
    workflow_url = 'https://api.github.com/repos/your_username/your_repo/actions/workflows/print_temperature.yml/dispatches'
    payload = {
        'ref': 'main',
        'inputs': {
            'temperature': str(temperature)
        }
    }
    response = requests.post(workflow_url, json=payload, headers=headers)
    if response.status_code == 204:
        print("GitHub workflow triggered successfully")
    else:
        print(f"Failed to trigger GitHub workflow: {response.status_code}")

# Add this to your existing DAG
trigger_task = PythonOperator(
    task_id='trigger_github_workflow',
    python_callable=trigger_github_workflow,
    provide_context=True
)

fetch_task >> process_task >> trigger_task
```

```bash
curl -L \
  -X POST \
  -H "Accept: application/vnd.github+json" \
  -H "Authorization: Bearer GITHUB_TOKEN" \
  -H "X-GitHub-Api-Version: 2022-11-28" \
  https://api.github.com/repos/REPO_OWNER/REPO_NAME/actions/workflows/print_temperature.yml/dispatches \
  -d '{"ref":"main","inputs":{"temperature":"40"}}'
```

## Conclusion

In this lab, you learned how to use **Apache Airflow** for orchestrating tasks in machine learning pipelines, including fetching data, processing it, saving to cloud storage, and notifying via email. You also integrated Airflow with **GitHub Actions**, demonstrating how workflows in GitHub can be triggered using external inputs such as weather data from Airflow. These skills will form the foundation for building more complex orchestration pipelines.

---

## Useful Links

- [Airflow Documentation](https://airflow.apache.org/docs/apache-airflow/stable/index.html)
- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [VisualCrossing API](https://www.visualcrossing.com/weather-api)
- [Crontab Guru](https://crontab.guru/)
