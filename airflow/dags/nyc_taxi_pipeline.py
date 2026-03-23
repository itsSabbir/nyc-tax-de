# =============================================================================
# Airflow DAG: nyc_taxi_pipeline
# =============================================================================
# This is the "orchestrator" — it defines WHAT tasks to run and IN WHAT ORDER.
# Think of it like a recipe: step 1 (ingest data), step 2 (seed lookup table),
# step 3 (build dbt models), step 4 (run sanity checks).
#
# Airflow doesn't DO the work itself — it just calls other tools (Python, dbt,
# bash) in the right sequence, with retries and logging built in.
#
# The task flow is:
#   ingest_yellow_to_pg  -->  dbt_seed  -->  dbt_build  -->  sanity_checks
#
# HOW TO RUN:
#   Option A: Trigger from Airflow UI at http://localhost:8080
#   Option B: Use the PowerShell script (scripts/run_pipeline.ps1) which
#             runs these same steps outside of Airflow
# =============================================================================

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# PATH CONFIGURATION
# ---------------------------------------------------------------------------
# REPO_ROOT = the root of our project INSIDE the Docker container.
# When Airflow runs in Docker, the project is mounted at /opt/airflow/repo
# (see docker/compose.yml volume mounts). We read this from an env var so
# it's easy to override if the mount point ever changes.
# ---------------------------------------------------------------------------
REPO_ROOT = os.getenv("REPO_ROOT", "/opt/airflow/repo")

# dbt needs to be run from inside the dbt project directory (dbt/nyc_taxi/).
# This is one of the most common gotchas — if you run dbt from the wrong
# folder, it can't find your models or seeds.
DBT_DIR = f"{REPO_ROOT}/dbt/nyc_taxi"

# ---------------------------------------------------------------------------
# DEFAULT ARGS — settings that apply to ALL tasks in this DAG
# ---------------------------------------------------------------------------
default_args = {
    "owner": "de",                          # Shows up in Airflow UI as the task owner
    "retries": 2,                           # If a task fails, retry it up to 2 times
    "retry_delay": timedelta(minutes=2),    # Wait 2 minutes between retries
}


def run_sanity_checks():
    """
    Final verification step: connect directly to Postgres and check that
    our gold-layer tables actually have data in them.

    This runs INSIDE the Airflow container, so we connect to 'postgres'
    (the Docker service name) — not 'localhost'. Docker's internal DNS
    resolves service names to container IPs automatically.

    If any of these counts are 0, something went wrong upstream.
    """
    import psycopg2

    # Connect to the same Postgres instance defined in compose.yml
    conn = psycopg2.connect(
        host="postgres",    # Docker service name (NOT localhost — we're inside Docker)
        user="de",
        password="de",
        dbname="warehouse"
    )
    cur = conn.cursor()

    # Check each gold-layer table has rows
    cur.execute("select count(*) from analytics.fact_trips;")
    print(f"Fact Trips count: {cur.fetchone()[0]}")

    cur.execute("select count(*) from analytics.dim_zone;")
    print(f"Dim Zones count: {cur.fetchone()[0]}")

    cur.execute("select count(*) from analytics.agg_daily_pickup;")
    print(f"Agg Daily Pickup count: {cur.fetchone()[0]}")

    cur.close()
    conn.close()


# ---------------------------------------------------------------------------
# DAG DEFINITION
# ---------------------------------------------------------------------------
# The DAG is the "container" for all our tasks. It defines:
#   - dag_id:     Unique name (shows up in Airflow UI)
#   - schedule:   When to run (None = manual trigger only)
#   - catchup:    False = don't backfill missed runs from start_date to now
#   - tags:       Just for organizing/filtering in the Airflow UI
# ---------------------------------------------------------------------------
with DAG(
    dag_id="nyc_taxi_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,                          # Manual trigger only (no cron schedule)
    catchup=False,                          # Don't try to run for every day since start_date
    tags=["nyc", "taxi", "dbt"],
) as dag:

    # -----------------------------------------------------------------------
    # TASK 1: Ingest raw parquet data into Postgres (bronze layer)
    # -----------------------------------------------------------------------
    # Runs our Python script that reads the parquet file, cleans it lightly,
    # and bulk-inserts it into staging.yellow_trips_raw.
    #
    # The 'env' dict passes environment variables TO the bash command.
    # Our Python script reads these with os.getenv() to know where the
    # parquet file is and how to connect to Postgres.
    # -----------------------------------------------------------------------
    ingest = BashOperator(
        task_id="ingest_yellow_to_pg",
        bash_command=(
            f"cd {REPO_ROOT} && "                    # cd into project root first
            "python ingest/load_yellow_to_pg.py"      # then run the ingest script
        ),
        env={
            "PARQUET_PATH": f"{REPO_ROOT}/data/raw/yellow_tripdata_2024-01.parquet",
            "PG_HOST": "postgres",     # Docker service name
            "PG_USER": "de",
            "PG_PASS": "de",
            "PG_DB": "warehouse"
        },
    )

    # -----------------------------------------------------------------------
    # TASK 2: Load the taxi zone lookup CSV into Postgres via dbt seed
    # -----------------------------------------------------------------------
    # dbt seed takes CSV files in the seeds/ folder and loads them as tables.
    # This gives us a zone dimension table (borough, zone name, etc.) that
    # we join against trip data in the marts layer.
    #
    # --full-refresh = drop and recreate the seed table every time
    # --select taxi_zone_lookup = only seed this one file (not everything)
    # -----------------------------------------------------------------------
    dbt_seed = BashOperator(
        task_id="dbt_seed_zone_lookup",
        bash_command=f"cd {DBT_DIR} && dbt seed --full-refresh --select taxi_zone_lookup",
    )

    # -----------------------------------------------------------------------
    # TASK 3: Build all dbt models + run tests
    # -----------------------------------------------------------------------
    # "dbt build" does two things in one command:
    #   1. Runs all SQL models (staging views + mart tables)
    #   2. Runs all tests defined in .yml files (not_null, unique, ranges)
    #
    # If any test fails, the task fails, and the pipeline stops here.
    # This is the core of the "Transform" in ELT.
    # -----------------------------------------------------------------------
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_DIR} && dbt build",
    )

    # -----------------------------------------------------------------------
    # TASK 4: Sanity checks — verify the gold tables have data
    # -----------------------------------------------------------------------
    # This is our final safety net. Even if dbt says "success", we want to
    # actually query the tables and confirm rows exist. Uses PythonOperator
    # so we can connect to Postgres directly from inside the container.
    # -----------------------------------------------------------------------
    sanity = PythonOperator(
        task_id="sanity_checks",
        python_callable=run_sanity_checks,
    )

    # -----------------------------------------------------------------------
    # TASK DEPENDENCIES — defines the execution order
    # -----------------------------------------------------------------------
    # Read this as: ingest runs first, THEN dbt_seed, THEN dbt_build,
    # THEN sanity. If any step fails, everything downstream is skipped.
    # -----------------------------------------------------------------------
    ingest >> dbt_seed >> dbt_build >> sanity
