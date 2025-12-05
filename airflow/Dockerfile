# Use official Airflow image as base
FROM apache/airflow:2.9.0

# Copy requirements file into container
COPY requirements.txt /requirements.txt

# Install Python dependencies from requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Copy DAGs and plugins into container
COPY dags/ /opt/airflow/dags/
COPY plugins/ /opt/airflow/plugins/

# Set working directory
WORKDIR /opt/airflow