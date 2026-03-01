FROM apache/airflow:2.9.3

USER root
# Install git
RUN apt-get update && apt-get install -y --no-install-recommends git \
  && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
# Install dbt during the build, so it's permanently baked into the image
RUN pip install --no-cache-dir dbt-postgres==1.8.2