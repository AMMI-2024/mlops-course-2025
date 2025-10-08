#!/bin/bash

# Airflow Setup Script for VM
# This script installs and configures Apache Airflow

set -e

echo "=== Installing Python and pip ==="
sudo apt-get update
sudo apt-get install -y python3-pip python3-venv

echo "=== Creating virtual environment ==="
python3 -m venv ~/airflow-venv
source ~/airflow-venv/bin/activate

echo "=== Installing Airflow ==="
pip install --upgrade pip
pip install apache-airflow==2.7.3
pip install requests

echo "=== Setting up Airflow home directory ==="
export AIRFLOW_HOME=~/airflow
mkdir -p $AIRFLOW_HOME/dags

echo "=== Initializing Airflow database ==="
airflow db init

echo "=== Creating Airflow admin user ==="
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

echo "=== Setup complete! ==="
echo "To start Airflow:"
echo "1. Start webserver: airflow webserver -p 8080 -D"
echo "2. Start scheduler: airflow scheduler -D"
echo ""
echo "Access Airflow UI at: http://<your-vm-ip>:8080"
echo "Username: admin"
echo "Password: admin"

