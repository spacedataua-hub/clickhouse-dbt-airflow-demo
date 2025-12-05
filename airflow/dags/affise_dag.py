from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago
from datetime import datetime, time, timedelta
import os
import requests

# Импортируем функцию из коннектора
from airflow.plugins.affise_connector import fetch_and_store_conversions

# Расширенные аргументы по умолчанию
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,   # включаем уведомления при ошибке
    "email": ["alerts@yourdomain.com"],  # список получателей
    "email_on_retry": False,
    "retries": 2,               # больше попыток
    "retry_delay": timedelta(minutes=10),
    "sla": timedelta(hours=1),  # SLA: задача должна завершиться за 1 час
}

# Slack webhook (берём из .env)
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

# Callback для мониторинга (пример: Slack уведомления)
def notify_slack(context):
    task_id = context['task_instance'].task_id
    dag_id = context['dag'].dag_id
    execution_date = context['execution_date']
    message = (
        f":rotating_light: Task *{task_id}* in DAG *{dag_id}* failed!\n"
        f"Execution date: {execution_date}\n"
        f"Log URL: {context['task_instance'].log_url}"
    )
    if SLACK_WEBHOOK_URL:
        requests.post(SLACK_WEBHOOK_URL, json={"text": message})
    else:
        print("Slack webhook not configured. Message:", message)

with DAG(
    dag_id="affise_clickhouse_daily",
    description="Fetch conversions from Affise API and store in ClickHouse daily at 07:00",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="0 7 * * *",  # каждый день в 07:00
    catchup=False,
    max_active_runs=1,              # не запускать DAG параллельно
    tags=["affise", "clickhouse", "demo"],  # метки для удобства
) as dag:

    # Документация DAG
    dag.doc_md = """
    ### Affise → ClickHouse DAG
    This DAG fetches conversions from Affise API and loads them into ClickHouse daily at 07:00.
    It includes monitoring via email and Slack notifications.
    It will only run after the Affise healthcheck DAG has succeeded.
    """

    # Сенсор: ждём выполнения DAG "affise_healthcheck"
    wait_for_healthcheck = ExternalTaskSensor(
        task_id="wait_for_affise_healthcheck",
        external_dag_id="affise_healthcheck",   # имя DAG, который проверяет API
        external_task_id="check_affise_api",    # имя задачи внутри healthcheck DAG
        execution_date_fn=lambda dt: datetime.combine(dt.date(), time(6, 0)),  # ровно в 06:00
        poke_interval=60,        # проверять каждую минуту
        timeout=120,             # максимум 2 минуты → 2 проверки
        mode="poke",             # можно оставить poke, чтобы точно 2 раза проверил
    )

    run_affise_job = PythonOperator(
        task_id="fetch_and_store_conversions",
        python_callable=fetch_and_store_conversions,
        on_failure_callback=notify_slack,  # мониторинг
    )

    # Задачи: сначала ждём healthcheck, потом запускаем ETL
    wait_for_healthcheck >> run_affise_job