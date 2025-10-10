#!/bin/bash

# Start Airflow webserver and scheduler

# Activate virtual environment
source ~/airflow-venv/bin/activate

# Set Airflow home
export AIRFLOW_HOME=~/airflow

# Start webserver in daemon mode
echo "Starting Airflow webserver on port 8080..."
airflow webserver -p 8080 -D

# Start scheduler in daemon mode
echo "Starting Airflow scheduler..."
airflow scheduler -D

echo "Airflow started successfully!"
echo "Access the UI at: http://$(hostname -I | awk '{print $1}'):8080"
echo "Username: admin"
echo "Password: admin"

