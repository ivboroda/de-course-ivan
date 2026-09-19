-- Крок 2: silver.commits. Специфікація: ../../SPEC.md → «Крок 2».
-- Джерело: {{ ref('events') }}, лише PushEvent.
-- from_json(payload, PUSH_SCHEMA) → explode масиву commits → commit grain. PUSH_SCHEMA = var('push_schema').
-- Дедуп: один рядок на commit_sha, найраніший pushed_at.
-- Колонки: commit_sha, repo_name, pushed_by, branch, author_name, author_email, message,
--          is_distinct, pushed_at, is_merge_commit, message_subject, message_length
-- Пастка: `distinct` — reserved word, у DDL-схемі та доступі до поля потрібні backticks.

-- TODO: замініть заглушку на запит згідно зі SPEC.md
with pushes as (
    select
        event_id,
        repo_name,
        actor_login,
        created_at,
        from_json(payload, '{{ var("push_schema") }}') as p
    from {{ ref('events') }}
    where event_type = 'PushEvent'
),

exploded as (
    select
        event_id,
        repo_name,
        actor_login,
        created_at,
        regexp_replace(p.ref, '^refs/heads/', '') as branch,
        explode(p.commits) as c
    from pushes
),

commits_flat as (
    select
        event_id,
        repo_name,
        actor_login    as pushed_by,
        branch,
        created_at     as pushed_at,
        c.sha          as commit_sha,
        c.message      as message,
        c.`distinct`   as is_distinct,
        c.author.name  as author_name,
        c.author.email as author_email
    from exploded
),

deduped as (
    select
        *,
        row_number() over (
            partition by commit_sha
            order by pushed_at, event_id
        ) as rn
    from commits_flat
)

select
    commit_sha,
    repo_name,
    pushed_by,
    branch,
    author_name,
    author_email,
    message,
    is_distinct,
    pushed_at,
    message like 'Merge %'  as is_merge_commit,
    split(message, '\n')[0] as message_subject,
    length(message)         as message_length
from deduped
where rn = 1
