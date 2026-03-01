import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB   = os.getenv("PG_DB", "warehouse")
PG_USER = os.getenv("PG_USER", "de")
PG_PASS = os.getenv("PG_PASS", "de")

PARQUET_PATH = os.getenv("PARQUET_PATH", r"data\raw\yellow_tripdata_2024-01.parquet")

COLS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "tip_amount",
    "total_amount",
    "PULocationID",
    "DOLocationID",
]

RENAME = {
    "VendorID": "vendor_id",
    "PULocationID": "pu_location_id",
    "DOLocationID": "do_location_id",
}

INT_COLS = ["vendor_id", "passenger_count", "pu_location_id", "do_location_id"]
FLOAT_COLS = ["trip_distance", "fare_amount", "tip_amount", "total_amount"]

TARGET_COLS_SQL = """
(vendor_id, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count,
 trip_distance, fare_amount, tip_amount, total_amount, pu_location_id, do_location_id)
"""

def coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns=RENAME)

    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"], errors="coerce")
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"], errors="coerce")

    for c in INT_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce").round(0).astype("Int64")

    for c in FLOAT_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    return df

def clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime"])
    df = df[df["tpep_dropoff_datetime"] >= df["tpep_pickup_datetime"]]
    df = df[df["trip_distance"].fillna(0) >= 0]
    df = df[df["total_amount"].fillna(0) >= 0]
    return df

def chunker(seq_len: int, chunk_size: int):
    for start in range(0, seq_len, chunk_size):
        yield start, min(start + chunk_size, seq_len)

def main() -> None:
    print(f"Reading parquet: {PARQUET_PATH}")
    df = pd.read_parquet(PARQUET_PATH, columns=COLS)

    df = coerce_types(df)
    df = clean(df)
    df = df.replace({pd.NA: None}) # get rid of Natypes errors

    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS
    )
    conn.autocommit = False

    total = len(df)
    chunk_size = int(os.getenv("CHUNK_SIZE", "50000"))

    try:
        with conn.cursor() as cur:
            cur.execute("truncate table staging.yellow_trips_raw;")

            print(f"Inserting {total:,} rows in chunks of {chunk_size:,}")

            for start, end in chunker(total, chunk_size):
                chunk = df.iloc[start:end]

                # Convert to python-native objects (int/float/datetime/None) for psycopg2
                rows = chunk.where(pd.notnull(chunk), None).astype(object).values.tolist()

                execute_values(
                    cur,
                    f"insert into staging.yellow_trips_raw {TARGET_COLS_SQL} values %s",
                    rows,
                    page_size=10_000,
                )
                print(f"Inserted {end:,}/{total:,}")

        conn.commit()
        print(f"Loaded {total:,} rows into staging.yellow_trips_raw")
    finally:
        conn.close()

if __name__ == "__main__":
    main()