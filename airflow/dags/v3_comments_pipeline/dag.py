from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from pipeline import fetch_comments, export_suspicious_comments


"""
Task 3: Create dag "v3_comments_pipeline". Set start date to yesterday.

The dag should run daily at midnight (use cron expression in schedule). 

HINT: https://crontab.guru/
"""
with DAG(
    dag_id = "v3_comments_pipeline",
    start_date = datetime(2025,11,20),
    schedule_interval = "@daily",
    catchup = False,
) as dag:
    # define both tasks using PythonOperator
    fetch = PythonOperator(
        task_id = "fetch_com",
        python_callable = fetch_comments,
    )
    
    export = PythonOperator(
        task_id = "export_com",
        python_callable = export_suspicious_comments,
    )
    # Dependency: first fetch, then export
    
    fetch >> export