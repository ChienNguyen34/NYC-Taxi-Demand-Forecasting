-- models/marts/dimensions/dim_datetime.sql

-- 1. Tạo một bảng lịch 2021
with calendar as (
    select
        date_day
    from
        unnest(generate_date_array('2021-01-01', '2021-12-31', interval 1 day)) as date_day
),

-- 2. Join với các sự kiện
events as (
    select * from {{ ref('stg_events') }}
)

-- 3. Tạo các thuộc tính thời gian
select
    -- Khóa chính (ví dụ: 20210101)
    format_date('%Y%m%d', date_day) as date_id,
    date_day as full_date,
    extract(year from date_day) as year,
    extract(month from date_day) as month,
    extract(day from date_day) as day,
    extract(dayofweek from date_day) as day_of_week, -- 1(Sun) - 7(Sat)
    extract(dayofyear from date_day) as day_of_year,
    extract(quarter from date_day) as quarter,
    
    -- Cờ (flags)
    case
        when extract(dayofweek from date_day) in (1, 7) then true
        else false
    end as is_weekend,
    
    -- Join sự kiện
    coalesce(events.event_name, 'No Event') as event_name,
    coalesce(events.event_type, 'No Event') as event_type,
    case
        when events.event_date is not null then true
        else false
    end as is_holiday

from calendar
left join events
    on calendar.date_day = events.event_date

-- dbt run --select dim_datetime
