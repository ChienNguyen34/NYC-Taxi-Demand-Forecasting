-- models/staging/stg_weather.sql
-- Fake dữ liệu NOAA GSOD 2021 thành 2025 bằng cách offset +4 năm

select
    cast(stn as string) as station_id,
    
    -- FAKE: Offset date từ 2021 sang 2025 (cộng 1461 ngày = 4 năm)
    date_add(cast(date as date), INTERVAL 4 YEAR) as observation_date,
    
    -- Convert nhiệt độ từ Fahrenheit sang Celsius: (F - 32) * 5/9
    round((cast(temp as numeric) - 32) * 5 / 9, 2) as avg_temp_celsius,
    round((cast(max as numeric) - 32) * 5 / 9, 2) as max_temp_celsius,
    round((cast(min as numeric) - 32) * 5 / 9, 2) as min_temp_celsius,
    
    -- Convert lượng mưa từ inches sang mm: 1 inch = 25.4 mm
    round(cast(prcp as numeric) * 25.4, 2) as precipitation_mm,
    
    -- Có mưa/tuyết/sương mù không (convert sang boolean)
    case when cast(fog as int64) = 1 then true else false end as is_foggy,
    case when cast(rain_drizzle as int64) = 1 then true else false end as is_rainy,
    case when cast(snow_ice_pellets as int64) = 1 then true else false end as is_snowy

from {{ source('public_weather', 'gsod2021') }}

where
    -- Lọc các trạm quan trắc chính ở NYC
    -- 725030 = LaGuardia Airport (LGA)
    -- 744860 = JFK International Airport (JFK)
    -- 725053 = Newark (EWR) - (Hơi xa nhưng vẫn ảnh hưởng)
    stn in ('725030', '744860', '725053')

-- dbt run --select stg_weather
-- Note: Data from 2021 is offset by +1461 days to simulate 2025 data
