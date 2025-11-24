-- models/marts/dimensions/dim_weather.sql
-- Aggregates weather data into daily summary (using NOAA GSOD 2021 faked as 2025)

select
    -- Primary key: weather date
    observation_date as weather_date,
    
    -- Aggregate temperature metrics (already in Celsius from staging)
    avg(avg_temp_celsius) as avg_temp_celsius,
    max(max_temp_celsius) as max_temp_celsius,
    min(min_temp_celsius) as min_temp_celsius,
    
    -- Sum precipitation (already in mm from staging)
    sum(precipitation_mm) as total_precipitation_mm,
    
    -- Weather condition flags (any station reports = true for the day)
    logical_or(is_rainy) as had_rain,
    logical_or(is_snowy) as had_snow,
    logical_or(is_foggy) as had_fog

from {{ ref('stg_weather_unified') }} -- Union of NOAA (fake 2025) + Streaming API

group by
    observation_date

order by
    observation_date

-- dbt run --select dim_weather
-- Note: If streaming data exists for a date, it will be averaged together with NOAA data
