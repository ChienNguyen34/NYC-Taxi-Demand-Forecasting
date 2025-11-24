-- models/marts/facts/agg_hourly_demand_h3.sql
-- (NEW: Bảng đặc trưng cho ML)

with trips as (
    select
        picked_up_at,
        pickup_h3_id,
        datetime_id,
        weather_date
    from {{ ref('fct_trips') }}
),

dim_datetime as (
    select
        date_id,
        is_weekend,
        is_holiday
    from {{ ref('dim_datetime') }}
),

dim_weather as (
    select
        weather_date,
        avg_temp_celsius,
        total_precipitation_mm,
        had_rain,
        had_snow
    from {{ ref('dim_weather') }}
)

-- 1. Tổng hợp các chuyến đi theo giờ và ô H3
select
    -- Khóa chính của bảng (Time Series ID)
    trips.pickup_h3_id,
    timestamp_trunc(trips.picked_up_at, hour) as timestamp_hour,
    
    -- Target variable (biến mục tiêu)
    count(*) as total_pickups,
    
    -- Lấy các features (đặc trưng)
    -- Chúng ta dùng min() vì tất cả các hàng trong giờ đó đều có cùng 1 giá trị feature
    min(dim_datetime.is_weekend) as is_weekend,
    min(dim_datetime.is_holiday) as is_holiday,
    min(extract(dayofweek from trips.picked_up_at)) as day_of_week, -- 1(Sun) - 7(Sat)
    min(extract(hour from trips.picked_up_at)) as hour_of_day,
    
    min(dim_weather.avg_temp_celsius) as avg_temp_celsius,
    min(dim_weather.total_precipitation_mm) as total_precipitation_mm,
    min(dim_weather.had_rain) as had_rain,
    min(dim_weather.had_snow) as had_snow
    
from trips

-- Join các features
left join dim_datetime
    on trips.datetime_id = dim_datetime.date_id
    
left join dim_weather
    on trips.weather_date = dim_weather.weather_date

-- where
--     dim_weather.weather_date is not null -- Tạm bỏ filter để có đủ data train model

group by
    1, 2 -- Group by pickup_h3_id, timestamp_hour

-- dbt run --select agg_hourly_demand_h3