# NYC Taxi Data Engineering Pipeline (Local ELT: Postgres + dbt + Airflow + Docker)

This repository is a **local, end-to-end data engineering pipeline** that takes raw NYC Taxi **Parquet** files and produces **analytics-ready tables** (star-schema style) in Postgres using **dbt**, with **Airflow** orchestrating the run.

It is designed to be:
- **Explainable**: every layer maps to a common industry concept (bronze, silver, gold).
- **Reproducible**: Docker Compose brings up the same environment every time.
- **Resume-ready**: demonstrates ingestion, transformation, testing, orchestration, and operational checks.

---

## 1) What this pipeline delivers (business view)

| Item | Description |
|---|---|
| Target audience | Analytics engineers, data analysts, product, ops, and anyone needing daily taxi demand and revenue signals |
| Primary outputs | `analytics.fact_trips` (trip-level fact), `analytics.dim_zone` (zone dimension), `analytics.agg_daily_pickup` (daily metrics) |
| Example questions answered | “How many trips per day by borough?”, “Average fare trends?”, “p95 trip distance changes?” |
| Processing mode | Batch (single month demo), easy to extend to daily/hourly partitions |
| Quality controls | dbt schema tests (unique, not_null, accepted ranges) + final sanity rowcount queries |

---

## 2) End-to-end flow (system view)

```text
Raw Parquet (data/raw/*.parquet)
   |
   |  (Python ingest: pandas clean + bulk insert)
   v
Postgres bronze: staging.yellow_trips_raw
   |
   |  (dbt staging: type casts + time logic)
   v
Postgres silver: analytics.stg_yellow_trips (view)
   |
   |  (dbt marts: dim + fact + aggregates + tests)
   v
Postgres gold:
  - analytics.dim_zone
  - analytics.fact_trips
  - analytics.agg_daily_pickup
   |
   |  (Airflow orchestration: ingest -> dbt seed -> dbt build -> sanity)
   v
Repeatable, schedulable pipeline run
```

---

## 3) Data layers and tables (what “bronze/silver/gold” means here)

| Layer | Purpose | Where it lives | Objects |
|---|---|---|---|
| Bronze | Raw landing zone inside the warehouse (minimal structure, fast load) | Postgres | `staging.yellow_trips_raw` |
| Silver | Cleaned, typed, logically valid records (analysis-safe) | dbt model (usually view) | `analytics.stg_yellow_trips` |
| Gold (marts) | Business-ready star schema components | dbt models (tables) | `analytics.dim_zone`, `analytics.fact_trips` |
| Gold (agg) | Pre-aggregated metrics for dashboards | dbt model (table) | `analytics.agg_daily_pickup` |

---

## 4) Repository structure (what each folder is responsible for)

### Tree

```text
nyc-taxi-de/
├── airflow/
│   └── dags/
│       └── nyc_taxi_pipeline.py
├── dbt/
│   ├── profiles.yml
│   └── nyc_taxi/
│       ├── dbt_project.yml
│       ├── packages.yml
│       ├── package-lock.yml
│       ├── README.md
│       ├── .gitignore
│       ├── analyses/     (kept for dbt completeness)
│       ├── macros/       (kept for dbt completeness)
│       ├── models/
│       │   ├── sources.yml
│       │   ├── staging/
│       │   │   └── stg_yellow_trips.sql
│       │   └── marts/
│       │       ├── dim_zone.sql
│       │       ├── fact_trips.sql
│       │       ├── fact_trips.yml
│       │       ├── agg_daily_pickup.sql
│       │       └── agg_daily_pickup.yml
│       ├── seeds/
│       │   ├── taxi_zone_lookup.csv
│       │   └── schema.yml
│       ├── snapshots/    (kept for dbt completeness)
│       └── tests/        (kept for dbt completeness)
├── docker/
│   ├── compose.yml
│   └── airflow/
│       └── Dockerfile
├── ingest/
│   └── load_yellow_to_pg.py
├── scripts/
│   └── run_pipeline.ps1
├── .gitignore
└── README.md
```

### Responsibilities

| Folder | Owner concern | What belongs here |
|---|---|---|
| `docker/` | Infrastructure as code | Postgres, Airflow, MinIO definitions; ports; volumes; mounts |
| `ingest/` | Extract + Load (EL) | Python code that reads Parquet and loads Postgres bronze |
| `dbt/nyc_taxi/` | Transform + Test (T) | dbt models, seeds, schema tests, lineage |
| `airflow/dags/` | Orchestration | DAG definition, task order, retries, scheduling |
| `scripts/` | Operations | “one click” local execution, smoke tests, runbook helpers |

---

## 5) Quickstart (works even if you know nothing about the repo)

### Prereqs

| Requirement | Why |
|---|---|
| Docker Desktop | Runs Postgres + Airflow + (optional) MinIO locally |
| PowerShell | Runs `scripts/run_pipeline.ps1` (Windows path) |
| NYC Taxi Parquet file | Source dataset for ingest step |

### Step-by-step run

| Step | Command | Expected outcome |
|---|---|---|
| 1) Start all services | `docker compose -f docker/compose.yml up -d` | Containers running (Postgres, Airflow, MinIO) |
| 2) Run full pipeline (local) | `powershell -ExecutionPolicy Bypass -File scripts/run_pipeline.ps1` | Ingest loads bronze, dbt seeds, dbt builds models, sanity queries pass |
| 3) Verify final tables | See verification queries below | Non-zero counts, sensible daily aggregates |

---

## 6) Running modes (choose how you want to operate)

### Mode A: Run via PowerShell (fastest smoke test)

| Why use it | What it simulates |
|---|---|
| No clicking around | “CI-like” deterministic run that proves the system works |

Command:

```bash
powershell -ExecutionPolicy Bypass -File scripts/run_pipeline.ps1
```

### Mode B: Run via Airflow UI (orchestration demo)

| Step | Action |
|---|---|
| 1 | Open Airflow UI: `http://localhost:8080` |
| 2 | Find DAG: `nyc_taxi_pipeline` |
| 3 | Trigger run |
| 4 | Confirm task order and logs: ingest -> dbt seed -> dbt build -> sanity |

### Mode C: Run dbt directly inside the Airflow container (path-safe)

This avoids “wrong folder” mistakes.

```bash
docker exec -it de_airflow_web bash -lc "
  /home/airflow/.local/bin/dbt debug \
    --project-dir /opt/airflow/repo/dbt/nyc_taxi \
    --profiles-dir /opt/airflow/repo/dbt
"
docker exec -it de_airflow_web bash -lc "
  /home/airflow/.local/bin/dbt seed \
    --project-dir /opt/airflow/repo/dbt/nyc_taxi \
    --profiles-dir /opt/airflow/repo/dbt
"
docker exec -it de_airflow_web bash -lc "
  /home/airflow/.local/bin/dbt build \
    --project-dir /opt/airflow/repo/dbt/nyc_taxi \
    --profiles-dir /opt/airflow/repo/dbt
"
```

---

## 7) dbt conventions that prevent 90% of failures

| Concept | This repo’s rule | Why it matters |
|---|---|---|
| dbt project root | `dbt/nyc_taxi/` | dbt resolves `models/`, `seeds/` relative to project root |
| dbt profiles location | `dbt/profiles.yml` | Keeps credentials separate from project code |
| How to run dbt | Always specify both flags: `--project-dir` and `--profiles-dir` | Prevents “seed not found” and “profiles not found” errors |
| Seeds path | `dbt/nyc_taxi/seeds/*` | dbt only auto-loads seeds inside the project |

---

## 8) Ingestion details (what the Python step is doing)

| Topic | What it does | Why it exists |
|---|---|---|
| Column projection | Reads only needed columns | Reduces memory and load time |
| Type coercion | Normalizes timestamps, numerics | Prevents downstream cast issues in dbt |
| Row filtering | Drops null timestamps; removes negative values | Enforces basic data sanity early |
| Bulk insert | Uses `execute_values` in batches | High throughput load pattern |
| Idempotency model | Truncates bronze table before insert | Safe for full reload demos; not historical accumulation |

If you want history later: change truncate to partitioned loads + dedupe keys.

---

## 9) Orchestration details (Airflow DAG intent)

| Task | Operator | Responsibility | Success condition |
|---|---|---|---|
| ingest | `BashOperator` | Run Python ingest script | Bronze table non-zero |
| dbt_seed | `BashOperator` | Load `taxi_zone_lookup.csv` | Seed table non-zero |
| dbt_build | `BashOperator` | Build models + run tests | Models created, tests pass |
| sanity | `PythonOperator` | Query Postgres for rowcounts | Counts meet minimum expectations |

---

## 10) Verification queries (copy/paste)

Run against Postgres container:

```bash
docker exec -it de_postgres psql -U de -d warehouse -c "select count(*) from staging.yellow_trips_raw;"
docker exec -it de_postgres psql -U de -d warehouse -c "select count(*) from analytics.stg_yellow_trips;"
docker exec -it de_postgres psql -U de -d warehouse -c "select count(*) from analytics.fact_trips;"
docker exec -it de_postgres psql -U de -d warehouse -c "select count(*) from analytics.dim_zone;"
docker exec -it de_postgres psql -U de -d warehouse -c "select * from analytics.agg_daily_pickup order by pickup_day limit 20;"
```

If your user/db differs, read it from `docker/compose.yml` and update the `-U` and `-d`.

---

## 11) Common issues and deterministic fixes

| Symptom | Root cause | Fix |
|---|---|---|
| VS Code shows `Import "airflow..." could not be resolved` | Pylance is using your local Python, not the Docker Airflow Python | Attach VS Code to the running container (Dev Containers) or silence missing-import diagnostics |
| dbt seed not found | Ran dbt from `dbt/` instead of `dbt/nyc_taxi/` | Use `--project-dir dbt/nyc_taxi --profiles-dir dbt` |
| Airflow UI shows no DAG | DAG file not mounted or parsing error | Confirm `airflow/dags/nyc_taxi_pipeline.py` exists and is mounted in compose |
| Rowcount is 0 in bronze | Parquet path mismatch or file missing | Confirm local `data/raw/*.parquet` exists and is mounted into container |
| dbt builds but tables land in wrong schema | Profile schema mismatch | Update `dbt/profiles.yml` target schema, then rerun `dbt build` |

---

## 12) Roadmap (how to extend this into “production shaped”)

| Upgrade | What you add | Why it’s valuable |
|---|---|---|
| Incremental loads | Partition by month/day, merge strategy, dedupe key | Supports real backfills and history |
| Lakehouse layer | Actually land raw Parquet into MinIO | Demonstrates modern object storage pattern |
| Observability | Rowcount diffs, freshness checks, failure alerts | Production reliability signals |
| Data contracts | More schema tests + column docs | Strong analytics engineering practice |
| CI | dbt build/test on PRs | Real-world engineering workflow |

---

## 13) Resume talking points (what you can claim from this repo)

| Skill area | Proof in this repo |
|---|---|
| Batch ingestion | Parquet -> Postgres bulk load with batching |
| Warehousing | Bronze staging schema + analytics schema layout |
| Transformations | dbt staging + marts, materializations, lineage |
| Data quality | dbt schema tests + sanity checks |
| Orchestration | Airflow DAG, task dependencies, retries |
| Operations | One-command execution script + verification queries |

