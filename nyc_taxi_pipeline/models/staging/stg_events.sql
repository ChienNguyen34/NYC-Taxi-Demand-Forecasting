-- models/staging/stg_events.sql

select
    cast(event_date as date) as event_date,
    cast(event_name as string) as event_name,
    cast(event_type as string) as event_type

from {{ ref('events_calendar') }}

-- dbt run --select stg_events