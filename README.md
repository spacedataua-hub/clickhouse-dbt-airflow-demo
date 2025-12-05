# clickhouse-dbt-airflow-demo

A demonstration project for showcasing **Data Engineering skills**.  
This repository integrates **ClickHouse**, **dbt**, and **Airflow** into a reproducible analytics pipeline, designed as a portfolio project.

## 🎯 Purpose
- Clear example of modern data pipeline architecture  
- Orchestration with Airflow  
- Transformations with dbt  
- Analytics-ready modeling in ClickHouse  
- Reproducible workflows using Docker Compose and CI/CD  
- Monitoring, alerting, and healthcheck practices in Airflow  

## 🛠️ Tech Stack
- ClickHouse — high-performance analytical database  
- dbt — SQL-based data transformations and modeling  
- Airflow — workflow orchestration, scheduling, monitoring, and alerting  
- Docker Compose — containerized local environment  
- Slack / Email Alerts — notifications on DAG failures  

## 📦 Dependencies
- `airflow/requirements.txt` — Python packages for Airflow (providers, monitoring, utils)  
- `dbt/requirements.txt` — Python packages for dbt (dbt-core, dbt-clickhouse, dbt-utils)  

## 🐳 Custom Dockerfiles
- `airflow/Dockerfile` — builds Airflow image with dependencies and DAGs  
- `dbt/Dockerfile` — builds dbt image with dependencies and macros  

## 📂 Project Structure
- `airflow/dags/affise_dag.py` — Main ETL DAG (Affise → ClickHouse)  
- `airflow/dags/affise_healthcheck.py` — Healthcheck DAG for Affise API  
- `airflow/dags/dbt_model_run_daily.py` — DAG that runs the dbt model daily at 07:01, after successful completion of the Affise connector DAG. Includes Slack notifications and dbt testing.  
- `airflow/plugins/affise_connector.py` — Connector logic (API + ClickHouse)  
- `airflow/config/airflow.cfg` — Minimal Airflow configuration  
- `airflow/requirements.txt` — Airflow dependencies  
- `airflow/Dockerfile` — Custom Airflow image  
- `dbt/requirements.txt` — dbt dependencies  
- `dbt/Dockerfile` — Custom dbt image  
- `dbt/macros/convert_to_utc.sql` — Example macro for converting timestamps to UTC  
- `.env` — Environment variables (API keys, DB creds, Slack webhook)  
- `docker-compose.yml` — Local orchestration  
- `README.md` — Project documentation  

## 🚀 Airflow DAGs
- **affise_clickhouse_daily** — Runs daily at 07:00, fetches conversions from Affise API and stores them in ClickHouse. Includes retries, SLA, email alerts, and Slack notifications.  
- **affise_healthcheck** — Runs daily at 06:00, checks Affise API availability before the main ETL DAG. Alerts if API is unreachable.  
- **dbt_model_run_daily** — Runs daily at 07:01, executes the dbt model `final_model` with all dependencies resolved automatically.  
  - Waits for the completion of the `affise_clickhouse_daily` DAG (via `ExternalTaskSensor`).  
  - Runs `dbt test` after the model execution to validate data quality.  
  - Sends Slack and Email alerts if any task fails.  

## 📊 Monitoring & Alerts
- Email alerts — via `default_args` (`email_on_failure=True`)  
- Slack alerts — via `on_failure_callback` and webhook integration (`SLACK_WEBHOOK_URL` in `.env`)  
- Logs — stored locally, can be extended to ClickHouse or cloud storage  
- **SLA** — critical tasks can be configured with execution time limits (e.g., 30 minutes for the dbt DAG).  

## 🚀 Getting Started
1. Clone the repository:  
   ```bash
   git clone https://github.com/spacedataua-hub/clickhouse-dbt-airflow-demo.git
   cd clickhouse-dbt-airflow-demo