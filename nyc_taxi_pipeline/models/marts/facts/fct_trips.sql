-- models/marts/facts/fct_trips.sql

with trips_data as (
    select * from {{ ref('stg_taxi_trips') }}
),

dim_datetime as (
    select * from {{ ref('dim_datetime') }}
),

dim_location as (
    select * from {{ ref('dim_location') }}
),

dim_weather as (
    select * from {{ ref('dim_weather') }}
)

select
    -- Khóa (Keys)
    {{ dbt_utils.generate_surrogate_key(['trips_data.picked_up_at', 'trips_data.vendor_id', 'trips_data.total_amount']) }} as trip_id,
    
    trips_data.vendor_id,
    trips_data.payment_type_id,
    trips_data.rate_code_id,

    -- Khóa ngoại (Foreign Keys)
    dim_datetime.date_id as datetime_id,
    
    -- Gán H3 ID cho pickup và dropoff
    pickup_loc.h3_id as pickup_h3_id,
    dropoff_loc.h3_id as dropoff_h3_id,
    
    dim_weather.weather_date, -- Khóa ngoại cho thời tiết
    
    -- Thông tin chuyến đi
    trips_data.picked_up_at,
    trips_data.dropped_off_at,
    
    -- Số đo (Measures)
    trips_data.passenger_count,
    trips_data.trip_distance,
    timestamp_diff(trips_data.dropped_off_at, trips_data.picked_up_at, second) as trip_duration_seconds,
    
    trips_data.fare_amount,
    trips_data.extra_amount,
    trips_data.mta_tax,
    trips_data.tip_amount,
    trips_data.tolls_amount,
    trips_data.improvement_surcharge,
    trips_data.airport_fee,
    trips_data.total_amount

from
    trips_data
    
-- Join để lấy datetime_id
left join dim_datetime
    on dim_datetime.full_date = date(trips_data.picked_up_at)

-- Join để lấy pickup H3 ID
left join dim_location as pickup_loc
    on trips_data.pickup_location_id = pickup_loc.zone_id

-- Join để lấy dropoff H3 ID
left join dim_location as dropoff_loc
    on trips_data.dropoff_location_id = dropoff_loc.zone_id

-- Join để lấy weather key
left join dim_weather
    on dim_weather.weather_date = date(trips_data.picked_up_at)

where
    dim_weather.weather_date is not null -- Lọc bỏ những ngày không có dữ liệu thời tiết
    and pickup_loc.h3_id is not null     -- Lọc bỏ những chuyến xe có H3 không xác định
    and dropoff_loc.h3_id is not null

-- dbt run --select fct_trips