-- Крок 3: silver.pull_requests. Специфікація: ../../SPEC.md → «Крок 3».
-- Джерело: {{ ref('events') }}, лише PullRequestEvent. from_json(payload, PR_SCHEMA), PR_SCHEMA = var('pr_schema').
-- Грануляція: один рядок на (repo_name, pr_number) — стан з ОСТАННЬОЇ за часом події (row_number desc).
-- Колонки: repo_name, pr_number, title, author_login, state, is_merged, is_draft, opened_at,
--          closed_at, merged_at, additions, deletions, changed_files, commits_count, comments,
--          review_comments, author_association, label_names, last_action, last_event_at, churn, hours_open

-- TODO: замініть заглушку на запит згідно зі SPEC.md
with pr_events as (
    select
        event_id,
        repo_name,
        created_at as event_at,
        from_json(payload, '{{ var("pr_schema") }}') as p
    from {{ ref('events') }}
    where event_type = 'PullRequestEvent'
),

flattened as (
    select
        event_id,
        repo_name,
        event_at,
        p.action       as action,
        p.number       as pr_number,
        p.pull_request as pr
    from pr_events
),

ranked as (
    select
        *,
        row_number() over (
            partition by repo_name, pr_number
            order by event_at desc, event_id desc
        ) as rn
    from flattened
),

latest as (
    select * from ranked where rn = 1
)

select
    repo_name,
    pr_number,
    pr.title                    as title,
    pr.user.login                as author_login,
    pr.state                     as state,
    pr.merged                    as is_merged,
    pr.draft                     as is_draft,
    to_timestamp(pr.created_at)  as opened_at,
    to_timestamp(pr.closed_at)   as closed_at,
    to_timestamp(pr.merged_at)   as merged_at,
    pr.additions                 as additions,
    pr.deletions                 as deletions,
    pr.changed_files             as changed_files,
    pr.commits                   as commits_count,
    pr.comments                  as comments,
    pr.review_comments           as review_comments,
    pr.author_association        as author_association,
    pr.labels.name                as label_names,
    action                        as last_action,
    event_at                      as last_event_at,
    pr.additions + pr.deletions   as churn,
    (
        cast(coalesce(to_timestamp(pr.closed_at), event_at) as double)
        - cast(to_timestamp(pr.created_at) as double)
    ) / 3600 as hours_open
from latest
