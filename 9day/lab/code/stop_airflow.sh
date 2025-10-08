#!/bin/bash

# Stop Airflow webserver and scheduler

echo "Stopping Airflow services..."

# Kill webserver
pkill -f "airflow webserver"

# Kill scheduler
pkill -f "airflow scheduler"

echo "Airflow services stopped"

