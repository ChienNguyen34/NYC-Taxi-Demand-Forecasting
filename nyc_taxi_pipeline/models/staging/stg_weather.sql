-- models/staging/stg_weather.sql

select
    cast(stn as string) as station_id,
    cast(date as date) as observation_date,
    
    -- Nhiệt độ (Fahrenheit, sẽ chuyển đổi sau)
    cast(temp as numeric) as avg_temp_f,
    cast(max as numeric) as max_temp_f,
    cast(min as numeric) as min_temp_f,
    
    -- Lượng mưa (inches)
    cast(prcp as numeric) as precipitation_inches,
    
    -- Có mưa/tuyết/sương mù không (1 là có, 0 là không)
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
