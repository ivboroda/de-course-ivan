{{ config(tags=['reconcile']) }}
-- Рейтинг водія в dim_driver = рейтинг із найпізнішої за occurred_at події ride_accepted,
-- а не з тієї, що приїхала останньою. Перерахунок напряму з silver.events. Тест міжшаровий
-- (Silver + Gold), тож без тега reconcile жоден крок DAG-у його б не запустив.
with latest as (
    select distinct on (payload #>> '{driver,id}')
        payload #>> '{driver,id}'                      as driver_key,
        (payload #>> '{driver,rating}')::numeric(3, 2) as expected_rating
    from {{ ref('events') }}
    where event_type = 'ride_accepted'
    order by payload #>> '{driver,id}', occurred_at desc, event_id desc
)

select
    driver_key,
    d.latest_rating,
    l.expected_rating
from {{ ref('dim_driver') }} as d
full outer join latest as l using (driver_key)
where driver_key <> 'unknown'
  and d.latest_rating is distinct from l.expected_rating