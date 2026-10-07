-- gold.agg_zone_hourly — ЕТАП 2. Grain: (requested_hour, pickup_zone_key). SPEC.md, розділ 4.4.
--   * incremental; після будь-якого інкременту = перерахунок із fact_ride рядок у рядок
--   * подумайте: що перераховувати, коли поїздка змінила зону? Який ключ стабільний?

{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='requested_hour'
    )
}}

with touched_hours as (

    select distinct requested_hour
    from {{ ref('fact_ride') }}
    {% if is_incremental() %}
    where _ingested_at > {{ high_watermark() }}
    {% endif %}

)

select
    f.requested_hour,
    f.pickup_zone_key,
    count(*)                                                                               as rides_requested,
    count(*) filter (where f.status = 'completed')                                         as rides_completed,
    count(*) filter (where f.status = 'cancelled')                                         as rides_cancelled,
    coalesce(sum(f.total_amount) filter (where f.status = 'completed'), 0)::numeric(12, 2) as gross_revenue,
    coalesce(sum(f.tip_amount)   filter (where f.status = 'completed'), 0)::numeric(12, 2) as tips,
    avg(f.wait_seconds)::numeric(10, 2)                                                    as avg_wait_seconds,
    max(f._ingested_at)                                                                    as _ingested_at,
    '{{ run_started_at }}'::timestamptz                                                    as _loaded_at
from {{ ref('fact_ride') }} as f
inner join touched_hours as t on t.requested_hour = f.requested_hour
group by f.requested_hour, f.pickup_zone_key
