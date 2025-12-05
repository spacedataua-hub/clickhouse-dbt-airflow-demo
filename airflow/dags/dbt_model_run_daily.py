from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime, time
import os
import requests

# Аргументы по умолчанию
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["alerts@yourdomain.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Slack webhook (берём из .env)
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

# Callback для мониторинга (Slack уведомления)
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

# DAG: запуск dbt модели после DAG-а коннектора
with DAG(
    dag_id="dbt_model_run_daily",
    description="Запуск dbt модели каскадно каждый день в 07:01 после DAG-а Affise",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="1 7 * * *",  # каждый день в 07:01
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "clickhouse", "demo"],
) as dag:

    # Сенсор: ждём выполнения DAG-а коннектора Affise
    wait_for_affise_connector = ExternalTaskSensor(
        task_id="wait_for_affise_connector",
        external_dag_id="affise_clickhouse_daily",   # имя DAG-а коннектора
        external_task_id="fetch_and_store_conversions",  # задача внутри DAG-а
        execution_date_fn=lambda dt: datetime.combine(dt.date(), time(7, 0)),  
        poke_interval=60,   # проверять каждую минуту
        timeout=600,        # максимум 10 минут ожидания
        mode="poke",
    )

    # Запуск dbt модели
    run_dbt_model = BashOperator(
        task_id="run_dbt_model",
        bash_command="docker exec dbt_container dbt run --models final_model",
        on_failure_callback=notify_slack,
    )

    # Запуск тестов dbt
    test_dbt_model = BashOperator(
        task_id="test_dbt_model",
        bash_command="docker exec dbt_container dbt test --models final_model",
        on_failure_callback=notify_slack,
    )

    # Последовательность: сначала ждём DAG коннектора, потом run, потом test
    wait_for_affise_connector >> run_dbt_model >> test_dbt_model