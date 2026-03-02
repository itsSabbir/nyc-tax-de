from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

REPO_ROOT = os.getenv("REPO_ROOT", "/opt/airflow/repo")
DBT_DIR = f"{REPO_ROOT}/dbt/nyc_taxi"

default_args = {
    "owner": "de",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

# New python function to replace the docker exec commands
def run_sanity_checks():
    import psycopg2
    # Connect directly to the postgres container
    conn = psycopg2.connect(host="postgres", user="de", password="de", dbname="warehouse")
    cur = conn.cursor()
    
    cur.execute("select count(*) from analytics.fact_trips;")
    print(f"Fact Trips count: {cur.fetchone()[0]}")
    
    cur.execute("select count(*) from analytics.dim_zone;")
    print(f"Dim Zones count: {cur.fetchone()[0]}")
    
    cur.execute("select count(*) from analytics.agg_daily_pickup;")
    print(f"Agg Daily Pickup count: {cur.fetchone()[0]}")
    
    cur.close()
    conn.close()

with DAG(
    dag_id="nyc_taxi_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,  
    catchup=False,
    tags=["nyc", "taxi", "dbt"],
) as dag:

    ingest = BashOperator(
        task_id="ingest_yellow_to_pg",
        bash_command=(
            f"cd {REPO_ROOT} && "
            "python ingest/load_yellow_to_pg.py"
        ),
        env={
            "PARQUET_PATH": f"{REPO_ROOT}/data/raw/yellow_tripdata_2024-01.parquet",
            "PG_HOST": "postgres",
            "PG_USER": "de",
            "PG_PASS": "de",
            "PG_DB": "warehouse"
        },
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed_zone_lookup",
        bash_command=f"cd {DBT_DIR} && dbt seed --full-refresh --select taxi_zone_lookup",
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_DIR} && dbt build",
    )

    # Replaced BashOperator with PythonOperator
    sanity = PythonOperator(
        task_id="sanity_checks",
        python_callable=run_sanity_checks,
    )

    ingest >> dbt_seed >> dbt_build >> sanity