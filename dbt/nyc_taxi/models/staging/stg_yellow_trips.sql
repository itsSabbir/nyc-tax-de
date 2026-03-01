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

clean as (
    select
        vendor_id::bigint as vendor_id,
        tpep_pickup_datetime::timestamp as pickup_ts,
        tpep_dropoff_datetime::timestamp as dropoff_ts,
        passenger_count::bigint as passenger_count,
        trip_distance::double precision as trip_distance,
        fare_amount::double precision as fare_amount,
        tip_amount::double precision as tip_amount,
        total_amount::double precision as total_amount,
        pu_location_id::bigint as pu_location_id,
        do_location_id::bigint as do_location_id
    from src
    where tpep_pickup_datetime is not null
      and tpep_dropoff_datetime is not null
      and tpep_dropoff_datetime >= tpep_pickup_datetime
      and coalesce(trip_distance, 0) >= 0
      and coalesce(total_amount, 0) >= 0
      -- keep only Jan 2024 for now (matches your current load)
      and tpep_pickup_datetime >= '2024-01-01'::timestamp
      and tpep_pickup_datetime <  '2024-02-01'::timestamp
)

select * from clean