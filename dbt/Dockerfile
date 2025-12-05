# Use official dbt ClickHouse image as base
FROM ghcr.io/dbt-labs/dbt-clickhouse:latest

# Copy requirements file into container
COPY requirements.txt /requirements.txt

# Install Python dependencies from requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Copy dbt project files into container
COPY . /usr/app/dbt

# Set working directory to dbt project
WORKDIR /usr/app/dbt