"""
System Monitoring DAG
This DAG monitors system resources on the VM
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator


def check_memory_usage(**context):
    """Check memory usage and push to XCom"""
    import os
    
    # Read memory info (Linux only)
    try:
        with open('/proc/meminfo', 'r') as f:
            lines = f.readlines()
            mem_total = int(lines[0].split()[1])  # MemTotal
            mem_free = int(lines[1].split()[1])   # MemFree
            mem_used = mem_total - mem_free
            mem_percent = (mem_used / mem_total) * 100
            
            print(f"Total Memory: {mem_total / 1024:.2f} MB")
            print(f"Used Memory: {mem_used / 1024:.2f} MB")
            print(f"Memory Usage: {mem_percent:.2f}%")
            
            context['ti'].xcom_push(key='mem_percent', value=mem_percent)
            
            if mem_percent > 80:
                print("WARNING: Memory usage is high!")
    except FileNotFoundError:
        print("Memory info not available (not on Linux)")


def check_cpu_load(**context):
    """Check CPU load"""
    import os
    
    # Get load average
    load1, load5, load15 = os.getloadavg()
    
    print(f"Load Average - 1 min: {load1}, 5 min: {load5}, 15 min: {load15}")
    
    context['ti'].xcom_push(key='load_1min', value=load1)


def generate_report(**context):
    """Generate a simple system report"""
    mem_percent = context['ti'].xcom_pull(task_ids='check_memory', key='mem_percent')
    load_1min = context['ti'].xcom_pull(task_ids='check_cpu', key='load_1min')
    
    report = f"""
    ===== System Monitoring Report =====
    Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    Memory Usage: {mem_percent:.2f}%
    CPU Load (1 min): {load_1min}
    ====================================
    """
    
    print(report)
    
    # Save to file
    with open('/tmp/system_report.txt', 'w') as f:
        f.write(report)


# Default arguments
default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Define the DAG
with DAG(
    'system_monitoring_dag',
    default_args=default_args,
    description='Monitor system resources on VM',
    schedule_interval=timedelta(minutes=30),
    catchup=False,
    tags=['monitoring', 'vm'],
) as dag:

    start_task = BashOperator(
        task_id='start',
        bash_command='echo "Starting system monitoring at $(date)"',
    )

    memory_task = PythonOperator(
        task_id='check_memory',
        python_callable=check_memory_usage,
        provide_context=True,
    )

    cpu_task = PythonOperator(
        task_id='check_cpu',
        python_callable=check_cpu_load,
        provide_context=True,
    )

    disk_task = BashOperator(
        task_id='check_disk',
        bash_command='df -h / | tail -1',
    )

    report_task = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report,
        provide_context=True,
    )

    # Task dependencies
    start_task >> [memory_task, cpu_task, disk_task] >> report_task

