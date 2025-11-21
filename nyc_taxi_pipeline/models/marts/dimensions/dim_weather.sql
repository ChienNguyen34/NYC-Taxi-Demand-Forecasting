-- models/marts/dimensions/dim_weather.sql
-- This model aggregates fine-grained streaming weather data into a daily summary.

select
    -- The primary key remains the date.
    observation_date as weather_date,
    
    -- Aggregate the weather metrics for the day.
    -- The data from stg_streaming_weather is already in Celsius, so no conversion is needed.
    avg(avg_temp_celsius) as avg_temp_celsius,
    max(max_temp_celsius) as max_temp_celsius,
    min(min_temp_celsius) as min_temp_celsius,
    
    -- Sum the precipitation. The data is already in mm.
    sum(precipitation_mm) as total_precipitation_mm,
    
    -- Determine if the day had specific weather conditions.
    LOGICAL_OR(is_rainy) as had_rain,
    LOGICAL_OR(is_snowy) as had_snow,
    LOGICAL_OR(is_foggy) as had_fog

from {{ ref('stg_streaming_weather') }} -- Changed source to the new streaming model

group by
    observation_date

order by
    observation_date

-- dbt run --select dim_weather
