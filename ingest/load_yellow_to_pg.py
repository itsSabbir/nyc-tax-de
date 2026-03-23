# =============================================================================
# Ingestion Script: Load NYC Yellow Taxi Parquet into Postgres (Bronze Layer)
# =============================================================================
# This script is the "E" and "L" in ELT (Extract + Load):
#   1. EXTRACT: Read .parquet files (columnar format, very fast)
#   2. CLEAN:   Light data quality fixes (null timestamps, negative amounts)
#   3. LOAD:    Bulk insert into Postgres table: staging.yellow_trips_raw
#
# The "T" (Transform) happens later in dbt — we intentionally keep this
# script simple and push complex logic into SQL where it's easier to test.
#
# MODES OF OPERATION:
#   Single file:  Set PARQUET_PATH env var to load one specific file
#   Multi-file:   Set PARQUET_DIR env var to load ALL .parquet files in a folder
#   Default:      Loads all files from data/raw/ (relative to project root)
#
# HOW THIS GETS CALLED:
#   - By Airflow (Task 1 in the DAG) with env vars for paths + credentials
#   - By the PowerShell script (scripts/run_pipeline.ps1) for local runs
#   - Manually: python ingest/load_yellow_to_pg.py
#
# ENVIRONMENT VARIABLES (all optional, have sensible defaults):
#   PARQUET_PATH  — path to a SINGLE .parquet file to load (overrides PARQUET_DIR)
#   PARQUET_DIR   — path to a FOLDER of .parquet files to load all of them
#   PG_HOST       — Postgres hostname (default: localhost for local, "postgres" in Docker)
#   PG_PORT       — Postgres port (default: 5432)
#   PG_DB         — database name (default: warehouse)
#   PG_USER       — database user (default: de)
#   PG_PASS       — database password (default: de)
#   CHUNK_SIZE    — rows per insert batch (default: 50000)
#   TRUNCATE      — set to "true" to wipe the table before loading (default: true)
# =============================================================================

import glob
import os
import sys
from pathlib import Path

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

# ---------------------------------------------------------------------------
# FILE DISCOVERY
# ---------------------------------------------------------------------------
# Priority order for finding parquet files:
#   1. PARQUET_PATH  — load one specific file (backwards compatible)
#   2. PARQUET_DIR   — load all .parquet files in a folder
#   3. Default       — data/raw/ relative to the project root
# ---------------------------------------------------------------------------
PARQUET_PATH = os.getenv("PARQUET_PATH")
PARQUET_DIR  = os.getenv("PARQUET_DIR")

# Whether to truncate the target table before loading. Set to "false" to
# append instead (useful when loading files one at a time in a loop).
TRUNCATE = os.getenv("TRUNCATE", "true").lower() == "true"

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


def discover_parquet_files() -> list[str]:
    """
    Figure out which parquet files to load based on env vars.

    Returns a sorted list of file paths. Sorting ensures files are loaded
    in chronological order (yellow_tripdata_2024-01 before 2024-02, etc.)
    which makes logs easier to follow.
    """
    # Option 1: Single file explicitly specified
    if PARQUET_PATH:
        if not os.path.exists(PARQUET_PATH):
            print(f"ERROR: PARQUET_PATH does not exist: {PARQUET_PATH}")
            sys.exit(1)
        return [PARQUET_PATH]

    # Option 2: Directory of parquet files
    search_dir = PARQUET_DIR
    if not search_dir:
        # Default: data/raw/ relative to this script's parent (the project root)
        search_dir = str(Path(__file__).resolve().parent.parent / "data" / "raw")

    # Find all .parquet files in the directory
    pattern = os.path.join(search_dir, "*.parquet")
    files = sorted(glob.glob(pattern))

    if not files:
        print(f"ERROR: No .parquet files found in: {search_dir}")
        print(f"       Run 'python scripts/download_data.py' to download the data first.")
        sys.exit(1)

    return files


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


def load_single_file(filepath: str, conn, chunk_size: int) -> int:
    """
    Load one parquet file into staging.yellow_trips_raw.

    Returns the number of rows loaded. Each file is loaded in its own
    sub-transaction so that a failure on file 6 of 12 doesn't lose
    the work from files 1-5.
    """
    filename = os.path.basename(filepath)
    print(f"\n{'='*60}")
    print(f"Loading: {filename}")
    print(f"{'='*60}")

    df = pd.read_parquet(filepath, columns=COLS)
    raw_count = len(df)

    df = coerce_types(df)
    df = clean(df)
    clean_count = len(df)

    dropped = raw_count - clean_count
    print(f"  Rows in file:    {raw_count:>10,}")
    print(f"  After cleaning:  {clean_count:>10,}  (dropped {dropped:,} bad rows)")

    # Replace pandas NA/NaT with Python None so psycopg2 can handle them
    df = df.replace({pd.NA: None})

    with conn.cursor() as cur:
        for start, end in chunker(clean_count, chunk_size):
            chunk = df.iloc[start:end]
            rows = chunk.where(pd.notnull(chunk), None).astype(object).values.tolist()

            execute_values(
                cur,
                f"insert into staging.yellow_trips_raw {TARGET_COLS_SQL} values %s",
                rows,
                page_size=10_000,
            )
            print(f"  Inserted {end:>10,} / {clean_count:,}")

    conn.commit()
    print(f"  Committed {clean_count:,} rows from {filename}")
    return clean_count


def main() -> None:
    """
    Main pipeline: discover files -> optionally truncate -> load each file.

    Key design decisions:
      - TRUNCATE before loading (when TRUNCATE=true): makes the full run
        idempotent — safe to re-run from scratch at any time
      - Each file committed separately: if file 6 of 12 fails, you keep
        files 1-5 and can resume from file 6
      - Files are loaded in sorted order: chronological and predictable
    """
    files = discover_parquet_files()
    print(f"Found {len(files)} parquet file(s) to load")
    for f in files:
        print(f"  - {os.path.basename(f)}")

    chunk_size = int(os.getenv("CHUNK_SIZE", "50000"))

    # Connect to Postgres
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS
    )
    conn.autocommit = False

    try:
        # Optionally truncate before loading (default: yes)
        if TRUNCATE:
            with conn.cursor() as cur:
                cur.execute("truncate table staging.yellow_trips_raw;")
            conn.commit()
            print("\nTruncated staging.yellow_trips_raw")

        # Load each file
        total_rows = 0
        for filepath in files:
            rows = load_single_file(filepath, conn, chunk_size)
            total_rows += rows

        # Final summary
        print(f"\n{'='*60}")
        print(f"DONE: Loaded {total_rows:,} total rows from {len(files)} file(s)")
        print(f"{'='*60}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
