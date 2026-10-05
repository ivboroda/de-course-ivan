-- gold.dim_driver — ЕТАП 2. Grain: водій. SPEC.md, розділ 4.4.
--   * incremental, unique_key='driver_key', delete+insert; за подіями ride_accepted у ref('events')
--   * «останній» рейтинг — за occurred_at; перераховуйте водія з УСІЄЇ його історії, не з батча
--   * плюс член driver_key = 'unknown' (поїздки, скасовані до прийняття)

{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='driver_key'
    )
}}

with touched as (

    select distinct payload #>> '{driver,id}' as driver_key
    from {{ ref('events') }}
    where event_type = 'ride_accepted'
    {% if is_incremental() %}
      and _ingested_at > {{ high_watermark() }}
    {% endif %}

),

history as (

    select
        e.payload #>> '{driver,id}'                      as driver_key,
        e.payload #>> '{driver,vehicle,type}'            as vehicle_type,
        e.payload #>> '{driver,vehicle,medallion}'       as medallion,
        (e.payload #>> '{driver,rating}')::numeric(3, 2) as rating,
        e.event_id,
        e.occurred_at,
        e._ingested_at
    from {{ ref('events') }} as e
    inner join touched as t on t.driver_key = (e.payload #>> '{driver,id}')
    where e.event_type = 'ride_accepted'

),

drivers as (

    select
        driver_key,
        (array_agg(vehicle_type order by occurred_at desc, event_id desc))[1] as vehicle_type,
        (array_agg(medallion    order by occurred_at desc, event_id desc))[1] as medallion,
        (array_agg(rating       order by occurred_at desc, event_id desc))[1] as latest_rating,
        min(occurred_at)  as first_seen_at,
        max(occurred_at)  as last_seen_at,
        max(_ingested_at) as _ingested_at
    from history
    group by driver_key

),

unknown_member as (

    select
        'unknown'::text     as driver_key,
        'unknown'::text     as vehicle_type,
        null::text          as medallion,
        null::numeric(3, 2) as latest_rating,
        null::timestamptz   as first_seen_at,
        null::timestamptz   as last_seen_at,
        null::timestamptz   as _ingested_at

)

select *, '{{ run_started_at }}'::timestamptz as _loaded_at from drivers
union all
select *, '{{ run_started_at }}'::timestamptz as _loaded_at from unknown_member
