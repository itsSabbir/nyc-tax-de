# =============================================================================
# PowerShell Script: Run the entire NYC Taxi pipeline locally (no Airflow)
# =============================================================================
# This is the "one-click" way to run the whole pipeline from your terminal.
# It does the same thing as the Airflow DAG, but without Airflow — useful
# for quick smoke tests and debugging.
#
# WHAT IT DOES (in order):
#   1. Start Docker containers (Postgres, Airflow, MinIO)
#   2. Wait for Postgres to be ready (polls every 2 seconds)
#   3. Run the Python ingest script (parquet -> Postgres bronze layer)
#   4. Run dbt seed (load taxi_zone_lookup.csv into Postgres)
#   5. Run dbt build (create all models + run tests)
#   6. Run sanity checks (query gold tables to confirm they have data)
#
# HOW TO RUN:
#   powershell -ExecutionPolicy Bypass -File scripts/run_pipeline.ps1
#
# PREREQS:
#   - Docker Desktop running
#   - Python + dbt installed locally (or in a venv)
#   - Parquet file at data/raw/yellow_tripdata_2024-01.parquet
# =============================================================================

# Stop the script immediately if any command fails (like "set -e" in bash)
$ErrorActionPreference = "Stop"

# ---------------------------------------------------------------------------
# PATH SETUP
# ---------------------------------------------------------------------------
# $PSScriptRoot = the folder this script lives in (scripts/)
# We go up one level to get the project root, then build paths from there.
# Using Resolve-Path + Join-Path keeps things portable and avoids hardcoding.
# ---------------------------------------------------------------------------
$repo = Resolve-Path (Join-Path $PSScriptRoot "..")          # Project root (nyc-taxi-de/)
$compose = Join-Path $repo "docker\compose.yml"               # Docker Compose file
$dbtDir  = Join-Path $repo "dbt\nyc_taxi"                     # dbt project directory
$parquet = Join-Path $repo "data\raw\yellow_tripdata_2024-01.parquet"  # Raw data file

Write-Host "Repo: $repo"

# ---------------------------------------------------------------------------
# STEP 1: Start all Docker containers
# ---------------------------------------------------------------------------
# "up -d" starts containers in the background (detached mode).
# If containers are already running, this is a no-op (safe to re-run).
# ---------------------------------------------------------------------------
Write-Host "Starting infra..."
docker compose -f $compose up -d

# ---------------------------------------------------------------------------
# STEP 2: Wait for Postgres to be ready
# ---------------------------------------------------------------------------
# Postgres takes a few seconds to start accepting connections after the
# container is "running". We poll it by trying a simple query every 2 seconds.
# If it's not ready after 60 seconds (30 tries x 2 sec), we give up.
# ---------------------------------------------------------------------------
Write-Host "Waiting for Postgres..."
for ($i=1; $i -le 30; $i++) {
  try {
    docker exec -i de_postgres psql -U de -d warehouse -c "select 1;" | Out-Null
    Write-Host "Postgres is ready."
    break
  } catch {
    Start-Sleep -Seconds 2
    if ($i -eq 30) { throw "Postgres not ready after 60s." }
  }
}

# ---------------------------------------------------------------------------
# STEP 3: Run the Python ingest script
# ---------------------------------------------------------------------------
# This reads the parquet file and bulk-inserts it into staging.yellow_trips_raw.
# We set PARQUET_PATH as an env var so the script knows where the file is.
# NOTE: This runs Python LOCALLY on your machine (not inside Docker).
# ---------------------------------------------------------------------------
Write-Host "Running ingest..."
$env:PARQUET_PATH = $parquet
python (Join-Path $repo "ingest\load_yellow_to_pg.py")

# ---------------------------------------------------------------------------
# STEP 4 & 5: Run dbt seed + build
# ---------------------------------------------------------------------------
# Push-Location / Pop-Location = temporarily cd into the dbt project dir.
# dbt needs to run from inside its project folder to find models and seeds.
#
# dbt seed:  loads CSV files from seeds/ as Postgres tables
#   --full-refresh = drop and recreate (ensures clean state)
#   --select taxi_zone_lookup = only this one seed file
#
# dbt build: runs all models AND tests in dependency order
#   staging views -> mart tables -> schema tests
# ---------------------------------------------------------------------------
Write-Host "Running dbt seed + build..."
Push-Location $dbtDir
dbt seed --full-refresh --select taxi_zone_lookup
dbt build
Pop-Location

# ---------------------------------------------------------------------------
# STEP 6: Sanity checks — query the gold tables to confirm they have data
# ---------------------------------------------------------------------------
# These are quick SELECT COUNT(*) queries against the final analytics tables.
# If any return 0, something went wrong in one of the earlier steps.
# ---------------------------------------------------------------------------
Write-Host "Sanity checks..."
docker exec -i de_postgres psql -U de -d warehouse -c "select count(*) as trips from analytics.fact_trips;"
docker exec -i de_postgres psql -U de -d warehouse -c "select count(*) as zones from analytics.dim_zone;"
docker exec -i de_postgres psql -U de -d warehouse -c "select count(*) as rows from analytics.agg_daily_pickup;"

Write-Host "DONE: pipeline run completed successfully."
