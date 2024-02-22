{{
    config(
        materialized='table'
    )
}}

with fhv_trips_data as (
    select *,
    'FHV' as service_type
    from {{ ref('stg_fhv_data') }}
),
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where Borough != 'Unknown'
)
select
    fhv.tripid,
    fhv.dispatching_base_num,
    fhv.service_type,
    fhv.pickup_datetime,
    fhv.dropOff_datetime,
    --fhv_trips_data.PUlocationID,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    --fhv_trips_data.DOlocationID,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    fhv.SR_Flag,
    fhv.Affiliated_base_number
from fhv_trips_data as fhv
inner join dim_zones as pickup_zone 
    on pickup_zone.LocationID = fhv.PUlocationID
inner join dim_zones as dropoff_zone 
    on dropoff_zone.LocationID = fhv.DOlocationID