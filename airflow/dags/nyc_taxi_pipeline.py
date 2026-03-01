from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

REPO_ROOT = os.getenv("REPO_ROOT", "/opt/airflow/repo")  # set this in docker compose
DBT_DIR = f"{REPO_ROOT}/dbt/nyc_taxi"

default_args = {
    "owner": "de",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="nyc_taxi_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,  # keep manual for now; we schedule in a later step
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
            # include PG_* env here if your ingest uses them and Airflow needs them
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

    sanity = BashOperator(
        task_id="sanity_checks",
        bash_command=(
            'docker exec -i de_postgres psql -U de -d warehouse -c "select count(*) as trips from analytics.fact_trips;" && '
            'docker exec -i de_postgres psql -U de -d warehouse -c "select count(*) as zones from analytics.dim_zone;" && '
            'docker exec -i de_postgres psql -U de -d warehouse -c "select count(*) as rows from analytics.agg_daily_pickup;"'
        ),
    )

    ingest >> dbt_seed >> dbt_build >> sanity
