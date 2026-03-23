-- =============================================================================
-- Fact Model: fact_trips (Gold Layer)
-- =============================================================================
-- A "fact" table holds measurable events — in this case, individual taxi trips.
-- Each row = one trip, with all the numeric measures (fare, distance, etc.)
-- and foreign keys to dimensions (pickup zone, dropoff zone).
--
-- The key thing this model adds over staging is the trip_id — a unique
-- identifier for each trip. The raw data doesn't have one, so we generate
-- it by hashing together the columns that make a trip unique.
--
-- Depends on: stg_yellow_trips (the cleaned staging view)
--
-- Materialized as: TABLE (defined in dbt_project.yml under marts/)
--   Facts are queried heavily by analysts, so we want them stored as real
--   tables for fast reads.
-- =============================================================================

select
    -- Generate a unique trip ID by hashing key columns together.
    -- MD5 produces a 32-character hex string. We concatenate the columns
    -- that together make each trip unique (vendor + times + locations + amount).
    -- coalesce(..., '') prevents NULL from making the whole hash NULL.
    md5(
      coalesce(vendor_id::text,'') || '|' ||
      coalesce(pickup_ts::text,'') || '|' ||
      coalesce(dropoff_ts::text,'') || '|' ||
      coalesce(pu_location_id::text,'') || '|' ||
      coalesce(do_location_id::text,'') || '|' ||
      coalesce(total_amount::text,'')
    ) as trip_id,

    -- All the columns from the staging model, already cleaned and typed
    vendor_id,
    pickup_ts,
    dropoff_ts,
    passenger_count,
    trip_distance,
    fare_amount,
    tip_amount,
    total_amount,
    pu_location_id,       -- FK to dim_zone (pickup location)
    do_location_id        -- FK to dim_zone (dropoff location)

from {{ ref('stg_yellow_trips') }}
