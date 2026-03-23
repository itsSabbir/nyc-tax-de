-- =============================================================================
-- Dimension Model: dim_zone (Gold Layer)
-- =============================================================================
-- A "dimension" table holds descriptive attributes about an entity.
-- This one maps a numeric location_id to human-readable info:
--   location_id 161 -> Manhattan, Midtown Center, Yellow Zone
--
-- In a star schema, fact tables (like fact_trips) store the location_id
-- as a foreign key, and you JOIN to this dimension to get the names.
--
-- Data source: taxi_zone_lookup.csv (loaded by "dbt seed")
--   The {{ ref('taxi_zone_lookup') }} function points to the seeded table.
--
-- Materialized as: TABLE (defined in dbt_project.yml under marts/)
--   We use a table because dimension lookups should be fast, and this
--   data is tiny (~265 rows) so the storage cost is negligible.
-- =============================================================================

with src as (
  select
    location_id,
    borough,
    zone,
    service_zone
  from {{ ref('taxi_zone_lookup') }}   -- References the CSV seed table
)

select
  location_id::bigint   as location_id,    -- Zone ID (primary key, join target)
  borough::text         as borough,        -- e.g., Manhattan, Brooklyn, Queens
  zone::text            as zone,           -- e.g., Upper East Side North
  service_zone::text    as service_zone    -- e.g., Yellow Zone, Boro Zone, EWR
from src
where location_id is not null              -- Drop any rows missing an ID (shouldn't happen, but safety)
