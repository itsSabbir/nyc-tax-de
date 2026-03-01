select
    md5(
      coalesce(vendor_id::text,'') || '|' ||
      coalesce(pickup_ts::text,'') || '|' ||
      coalesce(dropoff_ts::text,'') || '|' ||
      coalesce(pu_location_id::text,'') || '|' ||
      coalesce(do_location_id::text,'') || '|' ||
      coalesce(total_amount::text,'')
    ) as trip_id,
    vendor_id,
    pickup_ts,
    dropoff_ts,
    passenger_count,
    trip_distance,
    fare_amount,
    tip_amount,
    total_amount,
    pu_location_id,
    do_location_id
from {{ ref('stg_yellow_trips') }}