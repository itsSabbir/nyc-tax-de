-- =============================================================================
-- Staging Model: stg_yellow_trips (Silver Layer)
-- =============================================================================
-- This is the FIRST dbt model in the pipeline. It reads from the raw bronze
-- table (staging.yellow_trips_raw) and produces a clean, typed, validated
-- dataset that downstream models can trust.
--
-- What "staging" means:
--   - Rename columns to a consistent naming convention
--   - Cast columns to their correct types (explicit is better than implicit)
--   - Filter out obviously bad rows (nulls, impossible values)
--   - Don't add business logic here — that belongs in the marts layer
--
-- Materialized as: VIEW (defined in dbt_project.yml)
--   Views are perfect for staging because they don't store data separately.
--   They just sit on top of the bronze table and apply cleaning on the fly.
--
-- Depends on: {{ source('raw', 'yellow_trips_raw') }}  (the Python-loaded table)
-- =============================================================================

-- STEP 1: Pull columns from the raw source table
-- We use a CTE ("Common Table Expression") to keep the query organized.
-- The {{ source() }} function generates the actual table reference
-- (staging.yellow_trips_raw) based on what we declared in sources.yml.
with src as (
    select
        vendor_id,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        passenger_count,
        trip_distance,
        fare_amount,
        tip_amount,
        total_amount,
        pu_location_id,
        do_location_id
    from {{ source('raw', 'yellow_trips_raw') }}
),

-- STEP 2: Cast types explicitly and apply quality filters
-- Even though the Python ingest script already does some cleaning, we
-- repeat it here because:
--   a) Defense in depth — if someone loads data another way, dbt still cleans it
--   b) Explicit casts make the schema crystal clear for anyone reading this
--   c) The date filter ensures we only process the month we expect
clean as (
    select
        vendor_id::bigint            as vendor_id,
        tpep_pickup_datetime::timestamp  as pickup_ts,       -- Renamed for brevity
        tpep_dropoff_datetime::timestamp as dropoff_ts,      -- Renamed for brevity
        passenger_count::bigint      as passenger_count,
        trip_distance::double precision  as trip_distance,
        fare_amount::double precision    as fare_amount,
        tip_amount::double precision     as tip_amount,
        total_amount::double precision   as total_amount,
        pu_location_id::bigint       as pu_location_id,      -- Pickup zone (joins to dim_zone)
        do_location_id::bigint       as do_location_id       -- Dropoff zone (joins to dim_zone)
    from src
    where tpep_pickup_datetime is not null           -- Must have a pickup time
      and tpep_dropoff_datetime is not null           -- Must have a dropoff time
      and tpep_dropoff_datetime >= tpep_pickup_datetime  -- Can't drop off before pickup
      and coalesce(trip_distance, 0) >= 0             -- No negative distances
      and coalesce(total_amount, 0) >= 0              -- No negative amounts
      -- Date filter: only keep January 2024 (matches our current parquet file).
      -- When you add more months, update or remove these bounds.
      and tpep_pickup_datetime >= '2024-01-01'::timestamp
      and tpep_pickup_datetime <  '2024-02-01'::timestamp
)

select * from clean
