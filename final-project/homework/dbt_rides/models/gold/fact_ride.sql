-- gold.fact_ride — ЕТАП 2. Grain: поїздка (accumulating snapshot). SPEC.md, розділ 4.4.
--   * incremental, unique_key='ride_id', delete+insert; беріть з ref('rides') те, що змінилося
--   * FK до вимірів через LEFT JOIN + COALESCE (-1 / 'unknown'): рядок не губиться
--   * requested_date = utc_date(requested_at); requested_hour = початок години за UTC

{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='ride_id'
    )
}}

with changed as (

    select r.*
    from {{ ref('rides') }} as r
    {% if is_incremental() %}
    left join {{ this }} as f on f.ride_id = r.ride_id
    where r._ingested_at > {{ high_watermark() }}
       or (f.driver_key = 'unknown' and r.driver_id is not null)
    {% endif %}

)

select
    c.ride_id,
    coalesce(pz.zone_key, -1)         as pickup_zone_key,
    coalesce(dz.zone_key, -1)         as dropoff_zone_key,
    coalesce(d.driver_key, 'unknown') as driver_key,
    c.rider_id,
    c.status,
    c.requested_at,
    c.accepted_at,
    c.started_at,
    c.completed_at,
    c.cancelled_at,
    c.paid_at,
    {{ utc_date('c.requested_at') }}  as requested_date,
    date_trunc('hour', c.requested_at at time zone 'UTC') at time zone 'UTC' as requested_hour,
    c.wait_seconds,
    c.trip_seconds,
    c.distance_km,
    c.fare_amount,
    c.surge_multiplier,
    c.tolls_amount,
    c.tip_amount,
    c.total_amount,
    c.currency,
    c.payment_method,
    c.payment_amount,
    c.cancelled_by,
    c.cancel_reason,
    c.cancel_stage,
    c._ingested_at,
    '{{ run_started_at }}'::timestamptz as _loaded_at
from changed as c
left join {{ ref('dim_zone') }}   as pz on pz.zone_key   = c.pickup_zone_id
left join {{ ref('dim_zone') }}   as dz on dz.zone_key   = c.dropoff_zone_id
left join {{ ref('dim_driver') }} as d  on d.driver_key  = c.driver_id
