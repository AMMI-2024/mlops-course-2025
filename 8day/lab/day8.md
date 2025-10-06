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
  - [\[BONUS\] Part 3: GitHub Workflow Integration](#bonus-part-3-github-workflow-integration)
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

### Task 2: Using Airflow Operators

Create a DAG using the `BashOperator` and `PythonOperator`.

### Task 3: Running a DAG every 5 minutes

In this task, you’ll modify your DAG to run every 5 minutes.

To achieve this, modify the `schedule_interval` in your DAG definition:

**Note:** You can use [crontab.guru](https://crontab.guru/) to quickly figure out a cron schedule expression you might need.

### Task 4: Using XCom for Task Communication

In this task, you'll learn how to use XCom (Cross-Communication) to pass data between tasks. We will use both a `PythonOperator` and a `BashOperator`.

#### What is `ti` in Airflow?

In Airflow, `ti` stands for **TaskInstance**. It is an object that represents a single run of a task. When using `XCom`, we often refer to `ti` because it allows us to **push** and **pull** values (such as variables) between tasks. For example, you can push a value using `ti.xcom_push()` and retrieve it in another task with `ti.xcom_pull()`.

#### XCom Task Example

1. **PythonOperator**: Push a value (e.g., the current timestamp) using `XCom`.
2. **BashOperator**: Pull the value using `XCom` and echo it in the Bash command.

---

## Part 2: Fetch, Process, Save

In this part, you'll fetch weather data from an API, process it, save it to a local file.

### Steps

1. **Create an account** for the [Visual Crossing](https://www.visualcrossing.com/) and get your API key.
2. **Fetch** weather data using the VisualCrossing API and save the temperature to XCom. You can use any location you want, for example London.
3. **Process** the temperature in a separate task/function to convert it to a different unit (i.e., Fahrenheit, Kelvin) and save it again to XCom. This is to demonstrate that you know how to work with XCom.
4. **Save** the processed data to a local file.

---

## [BONUS] Part 3: GitHub Workflow Integration

In this part, you'll write a simple GitHub Actions workflow and trigger it from Airflow.

### Task 1: GitHub Actions Workflow (`.github/workflows/print_temperature.yml`)

In this task, you’ll create a simple GitHub Actions workflow that will echo the temperature passed from Airflow.

### Task 2: Generate GitHub Token with Write Permission

- Go to your GitHub account settings under **Developer Settings > Personal Access Tokens**.
- Generate a new fine-grained token with **write access** to `actions` in your repository.

### Task 3: Airflow Task to Trigger GitHub Workflow

Now that your workflow is ready, trigger it from Airflow using the temperature fetched from the weather API.

## Conclusion

In this lab, you learned how to use **Apache Airflow** for orchestrating tasks in machine learning pipelines, including fetching data, processing it, saving to cloud storage, and notifying via email. You also integrated Airflow with **GitHub Actions**, demonstrating how workflows in GitHub can be triggered using external inputs such as weather data from Airflow. These skills will form the foundation for building more complex orchestration pipelines.

---

## Useful Links

- [Airflow Documentation](https://airflow.apache.org/docs/apache-airflow/stable/index.html)
- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [VisualCrossing API](https://www.visualcrossing.com/weather-api)
- [Crontab Guru](https://crontab.guru/)
