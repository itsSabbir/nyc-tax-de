with src as (
  select
    location_id,
    borough,
    zone,
    service_zone
  from {{ ref('taxi_zone_lookup') }}
)

select
  location_id::bigint as location_id,
  borough::text as borough,
  zone::text as zone,
  service_zone::text as service_zone
from src
where location_id is not null