-- models/staging/stg_streaming_weather.sql
-- Этот модельный файл преобразует сырые данные о погоде из формата JSON в структурированную промежуточную таблицу.

with source_data as (
    -- Источник данных - таблица с сырыми JSON-данными о погоде
    select
        raw_json,
        inserted_at
    from {{ source('raw_data', 'weather_api_data') }}
)

select
    -- Извлекаем и преобразуем временную метку в дату наблюдения
    cast(timestamp_seconds(cast(json_value(raw_json, '$.dt') as int64)) as date) as observation_date,

    -- Температура уже в градусах Цельсия, так как в API был указан параметр 'units=metric'
    cast(json_value(raw_json, '$.main.temp') as numeric) as avg_temp_celsius,
    cast(json_value(raw_json, '$.main.temp_max') as numeric) as max_temp_celsius,
    cast(json_value(raw_json, '$.main.temp_min') as numeric) as min_temp_celsius,

    -- Осадки в мм (дождь за последний час). Если данных нет, считаем 0.
    coalesce(cast(json_value(raw_json, '$.rain."1h"') as numeric), 0) as precipitation_mm,

    -- Флаги погоды, основанные на описании
    -- Проверяем, содержит ли главное описание погоды соответствующие ключевые слова
    case when lower(json_value(raw_json, '$.weather[0].main')) like '%rain%' then true else false end as is_rainy,
    case when lower(json_value(raw_json, '$.weather[0].main')) like '%snow%' then true else false end as is_snowy,
    case 
        when lower(json_value(raw_json, '$.weather[0].main')) in ('mist', 'smoke', 'haze', 'dust', 'fog', 'sand', 'ash', 'squall', 'tornado') then true 
        else false 
    end as is_foggy,
    
    -- Временная метка вставки для возможной отладки
    inserted_at

from source_data

-- dbt run --select stg_streaming_weather
