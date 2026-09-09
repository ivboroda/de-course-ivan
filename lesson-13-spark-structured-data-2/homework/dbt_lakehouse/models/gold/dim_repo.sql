-- Крок 5: gold.dim_repo. Специфікація: ../../SPEC.md → «Крок 5».
-- Джерело: {{ ref('events') }}. Грануляція: один рядок на репозиторій.
-- Колонки: repo_id (md5(repo_name)), repo_name, repo_owner, first_seen_at, last_seen_at,
--          event_count, is_forked (є хоч одна подія ForkEvent по цьому репо).

-- TODO: замініть заглушку на запит згідно зі SPEC.md
select
    md5(repo_name)                as repo_id,
    repo_name,
    repo_owner,
    min(created_at)                as first_seen_at,
    max(created_at)                as last_seen_at,
    count(*)                       as event_count,
    max(event_type = 'ForkEvent')  as is_forked
from {{ ref('events') }}
group by repo_name, repo_owner
