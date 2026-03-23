-- =============================================================================
-- Aggregate Model: agg_daily_pickup (Gold Layer)
-- =============================================================================
-- This is a pre-aggregated table designed for dashboards and quick analytics.
-- Instead of analysts running GROUP BY queries on millions of fact rows every
-- time, we pre-compute the most common aggregations here.
--
-- Grain (what each row represents):
--   One row per (pickup_date, borough, zone) combination.
--   Example: "2024-01-15, Manhattan, Upper East Side North"
--
-- Metrics:
--   - trips:             Total number of trips that day in that zone
--   - avg_total_amount:  Average fare for that day/zone combo
--   - p95_distance:      95th percentile trip distance (catches outlier rides)
--
-- Depends on: fact_trips (trip-level fact) + dim_zone (zone dimension)
--
-- Materialized as: TABLE (pre-computed for fast dashboard queries)
-- =============================================================================

select
  -- Truncate timestamp to just the date (drop the time portion)
  date_trunc('day', f.pickup_ts)::date as pickup_date,

  -- Join to dim_zone to get human-readable zone names.
  -- COALESCE handles trips with unknown zone IDs (shouldn't happen, but safe).
  coalesce(z.borough, 'Unknown') as borough,
  coalesce(z.zone, 'Unknown') as zone,

  -- Aggregate metrics
  count(*) as trips,                                                  -- How many trips
  avg(f.total_amount) as avg_total_amount,                            -- Average fare
  percentile_cont(0.95) within group (order by f.trip_distance) as p95_distance  -- 95th percentile distance

from {{ ref('fact_trips') }} f

-- LEFT JOIN so we keep trips even if the zone ID isn't in our lookup table.
-- An INNER JOIN would silently drop trips with unknown zones.
left join {{ ref('dim_zone') }} z
  on f.pu_location_id = z.location_id

-- GROUP BY 1,2,3 = group by the first three columns in the SELECT.
-- This is shorthand — some people prefer writing out the full column names.
group by 1, 2, 3
