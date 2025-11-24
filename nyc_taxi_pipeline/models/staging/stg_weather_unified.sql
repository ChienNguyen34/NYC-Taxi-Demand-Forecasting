-- models/staging/stg_weather_unified.sql
-- Union dữ liệu thời tiết từ 3 nguồn: NOAA public, fake_weather_2025, và Streaming API

with fake_weather as (
    -- Nguồn 1: Fake weather data cho Sept-Nov 2025 (từ script populate_weather_data.sql)
    select
        observation_date,
        avg_temp_celsius,
        max_temp_celsius,
        min_temp_celsius,
        precipitation_mm,
        is_rainy,
        is_snowy,
        is_foggy
    from `nyc-taxi-project-477115.staging.fake_weather_2025`
),

streaming_weather as (
    -- Nguồn 2: OpenWeather API streaming (nếu có data)
    select
        observation_date,
        avg_temp_celsius,
        max_temp_celsius,
        min_temp_celsius,
        precipitation_mm,
        is_rainy,
        is_snowy,
        is_foggy
    from {{ ref('stg_streaming_weather') }}
)

-- UNION cả 2 nguồn
select * from fake_weather
union all
select * from streaming_weather

-- dbt run --select stg_weather_unified
