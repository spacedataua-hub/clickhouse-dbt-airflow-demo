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
- GitHub Actions / CI/CD — automated builds and testing  
- Slack / Email Alerts — notifications on DAG failures  

## 📂 Project Structure
- `airflow/dags/affise_dag.py` — Main ETL DAG (Affise → ClickHouse)  
- `airflow/dags/affise_healthcheck.py` — Healthcheck DAG for Affise API  
- `airflow/plugins/affise_connector.py` — Connector logic (API + ClickHouse)  
- `airflow/config/airflow.cfg` — Minimal Airflow configuration  
- `.env` — Environment variables (API keys, DB creds, Slack webhook)  
- `docker-compose.yml` — Local orchestration  
- `README.md` — Project documentation  

## 🚀 Airflow DAGs
- **affise_clickhouse_daily** — Runs daily at 07:00, fetches conversions from Affise API and stores them in ClickHouse. Includes retries, SLA, email alerts, and Slack notifications.  
- **affise_healthcheck** — Runs daily at 06:00, checks Affise API availability before the main ETL DAG. Alerts if API is unreachable.  

## 📊 Monitoring & Alerts
- Email alerts — via `default_args` (`email_on_failure=True`)  
- Slack alerts — via `on_failure_callback` and webhook integration  
- Logs — stored locally, can be extended to ClickHouse or cloud storage  


## 🚀 Getting Started
1. Clone the repository:
   ```bash
   git clone https://github.com/spacedataua-hub/clickhouse-dbt-airflow-demo.git
   cd clickhouse-dbt-airflow-demo
