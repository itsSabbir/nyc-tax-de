# scripts/run_pipeline.ps1
$ErrorActionPreference = "Stop"

$repo = Resolve-Path (Join-Path $PSScriptRoot "..")
$compose = Join-Path $repo "docker\compose.yml"
$dbtDir  = Join-Path $repo "dbt\nyc_taxi"
$parquet = Join-Path $repo "data\raw\yellow_tripdata_2024-01.parquet"

Write-Host "Repo: $repo"
Write-Host "Starting infra..."
docker compose -f $compose up -d

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

Write-Host "Running ingest..."
$env:PARQUET_PATH = $parquet
python (Join-Path $repo "ingest\load_yellow_to_pg.py")

Write-Host "Running dbt seed + build..."
Push-Location $dbtDir
dbt seed --full-refresh --select taxi_zone_lookup
dbt build
Pop-Location

Write-Host "Sanity checks..."
docker exec -i de_postgres psql -U de -d warehouse -c "select count(*) as trips from analytics.fact_trips;" 
docker exec -i de_postgres psql -U de -d warehouse -c "select count(*) as zones from analytics.dim_zone;" 
docker exec -i de_postgres psql -U de -d warehouse -c "select count(*) as rows from analytics.agg_daily_pickup;" 

Write-Host "DONE: pipeline run completed successfully."