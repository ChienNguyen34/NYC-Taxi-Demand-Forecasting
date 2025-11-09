-- models/marts/dimensions/dim_weather.sql

select
    -- Khóa chính là ngày
    observation_date as weather_date,
    
    -- Chỉ số thời tiết (convert F to C)
    avg((avg_temp_f - 32) * 5.0/9.0) as avg_temp_celsius,
    max((max_temp_f - 32) * 5.0/9.0) as max_temp_celsius,
    min((min_temp_f - 32) * 5.0/9.0) as min_temp_celsius,
    
    -- Tổng lượng mưa (convert inches to mm)
    sum(precipitation_inches * 25.4) as total_precipitation_mm,
    
    -- Cờ (flag) cho biết ngày hôm đó có mưa/tuyết/sương mù không
    max(is_rainy) as had_rain,
    max(is_snowy) as had_snow,
    max(is_foggy) as had_fog

from {{ ref('stg_weather') }}

group by
    observation_date

-- dbt run --select dim_weather
