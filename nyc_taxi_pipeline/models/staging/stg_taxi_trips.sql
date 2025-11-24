-- models/staging/stg_taxi_trips.sql

select
    -- IDs (Tất cả đều là STRING trong nguồn)
    cast(vendor_id as string) as vendor_id,
    
    -- Timestamps - Shift +4 years (1461 days)
    TIMESTAMP_ADD(CAST(pickup_datetime AS TIMESTAMP), INTERVAL 1461 DAY) as picked_up_at,
    TIMESTAMP_ADD(CAST(dropoff_datetime AS TIMESTAMP), INTERVAL 1461 DAY) as dropped_off_at,
    -- Trip info
    cast(passenger_count as int64) as passenger_count,
    cast(trip_distance as numeric) as trip_distance,
    
    -- Location info (Là STRING trong nguồn)
    cast(pickup_location_id as string) as pickup_location_id,
    cast(dropoff_location_id as string) as dropoff_location_id,
    
    -- Payment info (Là STRING trong nguồn)
    cast(rate_code as string) as rate_code_id,
    cast(payment_type as string) as payment_type_id,
    
    -- Numeric payment info (ĐÃ BỔ SUNG CÁC CỘT THIẾU)
    cast(fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra_amount, -- Thêm cột này
    cast(mta_tax as numeric) as mta_tax,
    cast(tip_amount as numeric) as tip_amount,
    cast(tolls_amount as numeric) as tolls_amount,
    cast(imp_surcharge as numeric) as improvement_surcharge, -- Đổi tên cho rõ
    cast(airport_fee as numeric) as airport_fee,
    cast(total_amount as numeric) as total_amount

from {{ source('public_data', 'tlc_yellow_trips_2021') }}

where
    -- Lọc bỏ các chuyến đi rác (ví dụ: khoảng cách <= 0)
    trip_distance > 0
    and passenger_count > 0
    and total_amount > 0
    -- Filter for actual 2025 data (remove weird future dates)
    and pickup_datetime >= '2021-09-23'  -- Data từ 23/9/2021
    and pickup_datetime < '2021-11-24'   -- Đến hết 23/11/2021
    -- Filter valid location IDs (not null/empty)
    and pickup_location_id is not null
    and dropoff_location_id is not null
    and pickup_location_id != ''
    and dropoff_location_id != ''

-- dbt run --select stg_taxi_trips
