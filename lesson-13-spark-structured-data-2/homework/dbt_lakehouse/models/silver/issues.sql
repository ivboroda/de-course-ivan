-- Крок 4: silver.issues. Специфікація: ../../SPEC.md → «Крок 4».
-- Джерело: {{ ref('events') }}, типи IssuesEvent ТА IssueCommentEvent (обидва несуть issue{}).
-- from_json(payload, ISSUE_SCHEMA), ISSUE_SCHEMA = var('issue_schema').
-- Грануляція: один рядок на (repo_name, issue_number) — стан з останньої за часом події.
-- Колонки: repo_name, issue_number, title, author_login, state, opened_at, closed_at,
--          comments, label_names, comment_events_seen, last_event_at, hours_to_close

-- TODO: замініть заглушку на запит згідно зі SPEC.md
with issue_events as (
    select
        event_id,
        repo_name,
        event_type,
        created_at as event_at,
        from_json(payload, '{{ var("issue_schema") }}') as p
    from {{ ref('events') }}
    where event_type in ('IssuesEvent', 'IssueCommentEvent')
),

flattened as (
    select
        event_id,
        repo_name,
        event_type,
        event_at,
        p.action        as action,
        p.issue.number  as issue_number,
        p.issue         as issue
    from issue_events
),

comment_counts as (
    select
        repo_name,
        issue_number,
        count(*) as comment_events_seen
    from flattened
    where event_type = 'IssueCommentEvent'
    group by repo_name, issue_number
),

ranked as (
    select
        *,
        row_number() over (
            partition by repo_name, issue_number
            order by event_at desc, event_id desc
        ) as rn
    from flattened
),

latest as (
    select * from ranked where rn = 1
)

select
    l.repo_name,
    l.issue_number,
    l.issue.title                       as title,
    l.issue.user.login                   as author_login,
    l.issue.state                        as state,
    to_timestamp(l.issue.created_at)     as opened_at,
    to_timestamp(l.issue.closed_at)      as closed_at,
    l.issue.comments                     as comments,
    l.issue.labels.name                  as label_names,
    coalesce(cc.comment_events_seen, cast(0 as bigint)) as comment_events_seen,
    l.event_at                           as last_event_at,
    (
        cast(to_timestamp(l.issue.closed_at) as double)
        - cast(to_timestamp(l.issue.created_at) as double)
    ) / 3600 as hours_to_close
from latest l
left join comment_counts cc
    on l.repo_name = cc.repo_name and l.issue_number = cc.issue_number
