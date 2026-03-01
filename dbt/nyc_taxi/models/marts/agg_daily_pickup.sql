select
  date_trunc('day', f.pickup_ts)::date as pickup_date,
  coalesce(z.borough, 'Unknown') as borough,
  coalesce(z.zone, 'Unknown') as zone,
  count(*) as trips,
  avg(f.total_amount) as avg_total_amount,
  percentile_cont(0.95) within group (order by f.trip_distance) as p95_distance
from {{ ref('fact_trips') }} f
left join {{ ref('dim_zone') }} z
  on f.pu_location_id = z.location_id
group by 1,2,3