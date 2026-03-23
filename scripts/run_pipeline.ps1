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
#   3. Optionally download data if no parquet files exist yet
#   4. Run the Python ingest script (ALL parquet files -> Postgres bronze)
#   5. Run dbt seed (load taxi_zone_lookup.csv into Postgres)
#   6. Run dbt build (create all models + run tests)
#   7. Run sanity checks (query gold tables to confirm they have data)
#
# HOW TO RUN:
#   powershell -ExecutionPolicy Bypass -File scripts/run_pipeline.ps1
#
# PREREQS:
#   - Docker Desktop running
#   - Python + dbt installed locally (or in a venv)
#   - Run 'python scripts/download_data.py' first (or this script will prompt you)
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
$dataDir = Join-Path $repo "data\raw"                         # Raw parquet files

Write-Host "Repo: $repo"

# ---------------------------------------------------------------------------
# STEP 0: Check that data files exist
# ---------------------------------------------------------------------------
# If the data/raw/ folder is empty or missing, remind the user to download.
# We don't auto-download because it's ~600 MB and the user should opt in.
# ---------------------------------------------------------------------------
if (-not (Test-Path $dataDir)) {
    Write-Host "WARNING: data/raw/ folder does not exist."
    Write-Host "Run 'python scripts/download_data.py' to download the parquet files first."
    exit 1
}

$parquetFiles = Get-ChildItem -Path $dataDir -Filter "*.parquet"
if ($parquetFiles.Count -eq 0) {
    Write-Host "WARNING: No .parquet files found in data/raw/"
    Write-Host "Run 'python scripts/download_data.py' to download the data first."
    exit 1
}

Write-Host "Found $($parquetFiles.Count) parquet file(s) in data/raw/"

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
# With PARQUET_DIR set, the script loads ALL .parquet files in data/raw/.
# TRUNCATE=true wipes the table first (full reload, safe to re-run).
# NOTE: This runs Python LOCALLY on your machine (not inside Docker).
# ---------------------------------------------------------------------------
Write-Host "Running ingest (loading all parquet files)..."
$env:PARQUET_DIR = $dataDir
$env:TRUNCATE = "true"
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
# We also check how many distinct months we have to confirm full year loaded.
# ---------------------------------------------------------------------------
Write-Host "Sanity checks..."
docker exec -i de_postgres psql -U de -d warehouse -c "select count(*) as trips from analytics.fact_trips;"
docker exec -i de_postgres psql -U de -d warehouse -c "select count(*) as zones from analytics.dim_zone;"
docker exec -i de_postgres psql -U de -d warehouse -c "select count(*) as rows from analytics.agg_daily_pickup;"
docker exec -i de_postgres psql -U de -d warehouse -c "select count(distinct date_trunc('month', pickup_ts)) as months_loaded from analytics.fact_trips;"

Write-Host "DONE: pipeline run completed successfully."
