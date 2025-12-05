from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
import requests

# Загружаем ключ из окружения
AFFISE_API_KEY = os.getenv("AFFISE_API_KEY")
AFFISE_API_URL = "https://api.affise.com/3.0/conversions"

# Функция проверки доступности Affise API
def check_affise_api():
    headers = {"API-Key": AFFISE_API_KEY}
    try:
        response = requests.get(AFFISE_API_URL, headers=headers, timeout=10)
        if response.status_code == 200:
            print("✅ Affise API is reachable")
        else:
            raise Exception(f"Affise API healthcheck failed: {response.status_code}")
    except Exception as e:
        raise Exception(f"Affise API healthcheck error: {str(e)}")

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="affise_healthcheck",
    description="Healthcheck DAG for Affise API availability",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="0 6 * * *",  # каждый день в 06:00, до основного DAG
    catchup=False,
    max_active_runs=1,
    tags=["healthcheck", "affise", "monitoring"],
) as dag:

    # Документация DAG
    dag.doc_md = """
    ### Affise Healthcheck DAG
    This DAG checks if the Affise API is reachable before running the main ETL DAG.
    It runs daily at 06:00 and alerts if the API is unavailable.
    """

    check_api = PythonOperator(
        task_id="check_affise_api",
        python_callable=check_affise_api,
    )