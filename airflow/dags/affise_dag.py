from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Импортируем функцию из коннектора
from airflow.plugins.affise_connector import fetch_and_store_conversions

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="affise_clickhouse_daily",
    description="Fetch conversions from Affise API and store in ClickHouse daily at 07:00",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="0 7 * * *",  # каждый день в 07:00
    catchup=False,
) as dag:

    run_affise_job = PythonOperator(
        task_id="fetch_and_store_conversions",
        python_callable=fetch_and_store_conversions,  #  функция из affise_connector
    )