# NYC Taxi Data Engineering Pipeline (Local ELT)

This repo builds a local, end-to-end batch ELT pipeline:
Parquet (raw) -> Postgres (bronze) -> dbt (silver/gold) -> Airflow (orchestration).

## Business goal (why this exists)
Primary audience: analytics engineers, data analysts, and stakeholders.
Primary artifact: `analytics.agg_daily_pickup` for daily taxi activity metrics by borough.
Typical cadence: daily or hourly batch; this repo runs a single-month demo batch.

## Architecture (mental model)
- Bronze: raw landing table in Postgres (`staging.yellow_trips_raw`)
- Silver: cleaned staging model (`analytics.stg_yellow_trips`)
- Gold: star-schema marts (`analytics.dim_zone`, `analytics.fact_trips`)
- Gold aggregate: daily metrics (`analytics.agg_daily_pickup`)
- Orchestration: Airflow DAG runs ingest -> dbt seed -> dbt build -> sanity checks

## Repo structure

nyc-taxi-de/
- docker/
  - compose.yml
  - airflow/Dockerfile
- ingest/
  - load_yellow_to_pg.py
- dbt/
  - profiles.yml
  - nyc_taxi/
    - dbt_project.yml
    - models/
      - sources.yml
      - staging/stg_yellow_trips.sql
      - marts/dim_zone.sql
      - marts/fact_trips.sql
      - marts/agg_daily_pickup.sql
      - marts/*.yml
    - seeds/
      - taxi_zone_lookup.csv
      - schema.yml
- airflow/
  - dags/nyc_taxi_pipeline.py
- data/
  - raw/yellow_tripdata_2024-01.parquet
- scripts/
  - run_pipeline.ps1

## Quickstart (single command path)
Prereqs: Docker Desktop, PowerShell, and the raw Parquet file in `data/raw/`.

1) Start services
- From repo root:
  - `docker compose -f docker/compose.yml up -d`

2) Run the pipeline locally (outside Airflow)
- `powershell -ExecutionPolicy Bypass -File scripts/run_pipeline.ps1`

## Running through Airflow UI (orchestration path)
1) Open Airflow: http://localhost:8080
2) Enable DAG: `nyc_taxi_pipeline`
3) Trigger run, then monitor task logs.

## dbt commands (golden path inside the Airflow container)
Use explicit paths to avoid “wrong working directory” issues.

- dbt debug:
  docker exec -it de_airflow_web bash -lc "
    /home/airflow/.local/bin/dbt debug \
      --project-dir /opt/airflow/repo/dbt/nyc_taxi \
      --profiles-dir /opt/airflow/repo/dbt
  "

- dbt seed:
  docker exec -it de_airflow_web bash -lc "
    /home/airflow/.local/bin/dbt seed \
      --project-dir /opt/airflow/repo/dbt/nyc_taxi \
      --profiles-dir /opt/airflow/repo/dbt
  "

- dbt build (models + tests):
  docker exec -it de_airflow_web bash -lc "
    /home/airflow/.local/bin/dbt build \
      --project-dir /opt/airflow/repo/dbt/nyc_taxi \
      --profiles-dir /opt/airflow/repo/dbt
  "

## Data outputs (what to query)
- Bronze:
  - staging.yellow_trips_raw
- Silver:
  - analytics.stg_yellow_trips
- Gold:
  - analytics.dim_zone
  - analytics.fact_trips
  - analytics.agg_daily_pickup

Example verification queries:
- `select count(*) from staging.yellow_trips_raw;`
- `select count(*) from analytics.fact_trips;`
- `select * from analytics.agg_daily_pickup order by pickup_day limit 20;`

## Operational guarantees (what is “safe”)
- Ingest truncates the bronze table before load.
- That makes the run idempotent for full reload batches.
- Tradeoff: you lose historical accumulation unless you go incremental.

## Common failures (and deterministic fixes)

### Seed file not found
Symptom: dbt cannot find `taxi_zone_lookup.csv`.
Fix:
- Seeds must exist at: `dbt/nyc_taxi/seeds/taxi_zone_lookup.csv`
- Run dbt with:
  - `--project-dir .../dbt/nyc_taxi`
  - not the parent `dbt/` folder

### dbt debug works but dbt seed fails
Cause: wrong working directory or wrong `profiles-dir`.
Fix:
- Keep profiles at: `/opt/airflow/repo/dbt/profiles.yml`
- Always pass `--profiles-dir /opt/airflow/repo/dbt`

### Rowcount is 0 after ingest
Cause set:
- Parquet path mismatch.
- Filters dropped most rows.
Fix:
- Confirm file exists in container mount.
- Print ingest rowcount before insert.
