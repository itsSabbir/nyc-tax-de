# =============================================================================
# Ingestion Script: Load NYC Yellow Taxi Parquet into Postgres (Bronze Layer)
# =============================================================================
# This script is the "E" and "L" in ELT (Extract + Load):
#   1. EXTRACT: Read a .parquet file (columnar format, very fast)
#   2. CLEAN:   Light data quality fixes (null timestamps, negative amounts)
#   3. LOAD:    Bulk insert into Postgres table: staging.yellow_trips_raw
#
# The "T" (Transform) happens later in dbt — we intentionally keep this
# script simple and push complex logic into SQL where it's easier to test.
#
# HOW THIS GETS CALLED:
#   - By Airflow (Task 1 in the DAG) with env vars for paths + credentials
#   - By the PowerShell script (scripts/run_pipeline.ps1) for local runs
#   - Manually: python ingest/load_yellow_to_pg.py
#
# ENVIRONMENT VARIABLES (all optional, have sensible defaults):
#   PARQUET_PATH  — path to the .parquet file to load
#   PG_HOST       — Postgres hostname (default: localhost for local, "postgres" in Docker)
#   PG_PORT       — Postgres port (default: 5432)
#   PG_DB         — database name (default: warehouse)
#   PG_USER       — database user (default: de)
#   PG_PASS       — database password (default: de)
#   CHUNK_SIZE    — rows per insert batch (default: 50000)
# =============================================================================

import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# ---------------------------------------------------------------------------
# CONNECTION SETTINGS
# ---------------------------------------------------------------------------
# We read everything from environment variables with sensible defaults.
# This pattern makes the script work both locally (defaults) and inside
# Docker (where Airflow sets these env vars in the DAG definition).
# ---------------------------------------------------------------------------
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB   = os.getenv("PG_DB", "warehouse")
PG_USER = os.getenv("PG_USER", "de")
PG_PASS = os.getenv("PG_PASS", "de")

# Path to the raw parquet file. The default uses a Windows-style relative
# path for running locally. When Airflow calls this, it overrides with an
# absolute Linux path inside the container.
PARQUET_PATH = os.getenv("PARQUET_PATH", r"data\raw\yellow_tripdata_2024-01.parquet")

# ---------------------------------------------------------------------------
# COLUMN CONFIGURATION
# ---------------------------------------------------------------------------
# We only read the columns we actually need from the parquet file.
# This is called "column projection" — it saves memory and speeds up loading
# because parquet is columnar (it can skip columns we don't ask for).
# ---------------------------------------------------------------------------
COLS = [
    "VendorID",                # Which taxi company (1=Creative Mobile, 2=VeriFone)
    "tpep_pickup_datetime",    # When the passenger was picked up
    "tpep_dropoff_datetime",   # When the passenger was dropped off
    "passenger_count",         # Number of passengers
    "trip_distance",           # Trip distance in miles
    "fare_amount",             # Base fare charged by the meter
    "tip_amount",              # Tip amount (only recorded for credit card payments)
    "total_amount",            # Total charged to the passenger
    "PULocationID",            # Pickup zone ID (joins to taxi_zone_lookup)
    "DOLocationID",            # Dropoff zone ID (joins to taxi_zone_lookup)
]

# The raw parquet uses CamelCase column names. We rename them to snake_case
# to match our Postgres table schema and general SQL convention.
RENAME = {
    "VendorID": "vendor_id",
    "PULocationID": "pu_location_id",
    "DOLocationID": "do_location_id",
}

# Columns that should be integers (zone IDs, vendor, passenger count)
INT_COLS = ["vendor_id", "passenger_count", "pu_location_id", "do_location_id"]

# Columns that should be floats (money amounts, distance)
FLOAT_COLS = ["trip_distance", "fare_amount", "tip_amount", "total_amount"]

# The column list for the SQL INSERT statement.
# Must match the exact column order in the DataFrame.
TARGET_COLS_SQL = """
(vendor_id, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count,
 trip_distance, fare_amount, tip_amount, total_amount, pu_location_id, do_location_id)
"""


def coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Force columns to the correct types.

    Why this is needed: Parquet files from the NYC TLC site sometimes have
    messy data — passenger_count might be a float (1.0 instead of 1), or
    timestamps might be stored as strings. We normalize everything here
    so Postgres doesn't throw type errors during the bulk insert.

    Int64 (capital I) is pandas' nullable integer type — regular int can't
    hold NaN/None, but Int64 can. We need this because some rows have
    missing passenger counts.
    """
    df = df.rename(columns=RENAME)

    # Parse timestamps — 'coerce' means invalid dates become NaT (Not a Time)
    # instead of throwing an error
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"], errors="coerce")
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"], errors="coerce")

    # Convert IDs to nullable integers (round first because some are floats like 1.0)
    for c in INT_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce").round(0).astype("Int64")

    # Convert money/distance columns to floats
    for c in FLOAT_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    return df


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove obviously bad rows.

    We apply minimal cleaning here — just enough to prevent garbage from
    entering the bronze layer. More sophisticated cleaning happens in dbt
    (the staging model stg_yellow_trips.sql).

    Rules:
      - Must have both pickup and dropoff timestamps (otherwise useless)
      - Dropoff must be >= pickup (can't arrive before you leave)
      - Distance and total amount must be non-negative (no refund rows)
    """
    df = df.dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime"])
    df = df[df["tpep_dropoff_datetime"] >= df["tpep_pickup_datetime"]]
    df = df[df["trip_distance"].fillna(0) >= 0]
    df = df[df["total_amount"].fillna(0) >= 0]
    return df


def chunker(seq_len: int, chunk_size: int):
    """
    Yield (start, end) index pairs for processing data in chunks.

    Why chunking: Inserting 2M+ rows in one shot can blow up memory and
    cause Postgres to hold a huge transaction lock. By inserting in chunks
    of 50K rows, we keep memory usage reasonable and can see progress.
    """
    for start in range(0, seq_len, chunk_size):
        yield start, min(start + chunk_size, seq_len)


def main() -> None:
    """
    Main pipeline: read parquet -> clean -> bulk insert into Postgres.

    Key design decisions:
      - TRUNCATE before insert: makes this script idempotent (safe to re-run)
      - Single transaction: if anything fails mid-insert, the whole thing
        rolls back cleanly (no partial data in the table)
      - Chunked inserts: memory-friendly and shows progress as it goes
    """
    print(f"Reading parquet: {PARQUET_PATH}")
    df = pd.read_parquet(PARQUET_PATH, columns=COLS)

    df = coerce_types(df)
    df = clean(df)

    # Replace pandas NA/NaT with Python None so psycopg2 can handle them.
    # Without this, you get "can't adapt type NAType" errors.
    df = df.replace({pd.NA: None})

    # Connect to Postgres with autocommit OFF so we get a single transaction
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS
    )
    conn.autocommit = False

    total = len(df)
    chunk_size = int(os.getenv("CHUNK_SIZE", "50000"))

    try:
        with conn.cursor() as cur:
            # Truncate = delete all existing rows.
            # This makes the script safe to re-run (idempotent).
            # For production, you'd use an upsert or partitioned approach instead.
            cur.execute("truncate table staging.yellow_trips_raw;")

            print(f"Inserting {total:,} rows in chunks of {chunk_size:,}")

            for start, end in chunker(total, chunk_size):
                chunk = df.iloc[start:end]

                # Convert pandas types to plain Python types (int, float, datetime, None)
                # because psycopg2 doesn't understand pandas' special types like Int64
                rows = chunk.where(pd.notnull(chunk), None).astype(object).values.tolist()

                # execute_values is much faster than executemany for bulk inserts.
                # page_size controls how many rows are sent per round-trip to Postgres.
                execute_values(
                    cur,
                    f"insert into staging.yellow_trips_raw {TARGET_COLS_SQL} values %s",
                    rows,
                    page_size=10_000,
                )
                print(f"Inserted {end:,}/{total:,}")

        # If we made it here without errors, commit the whole transaction
        conn.commit()
        print(f"Loaded {total:,} rows into staging.yellow_trips_raw")
    finally:
        # Always close the connection, even if something crashed
        conn.close()


if __name__ == "__main__":
    main()
