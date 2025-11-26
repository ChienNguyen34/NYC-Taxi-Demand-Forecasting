-- populate_streaming_trips.sql
-- Script INSERT manual data vào streaming.processed_trips để test pipeline
-- Lấy 100K trips từ public dataset 2021, shift time sang 2025 với FULL fields

INSERT INTO `nyc-taxi-project-477115.streaming.processed_trips` 
(vendor_id, pickup_datetime, dropoff_datetime, passenger_count, trip_distance, 
 pickup_location_id, dropoff_location_id, rate_code, payment_type,
 fare_amount, extra, mta_tax, tip_amount, tolls_amount, imp_surcharge, 
 airport_fee, total_amount, processing_timestamp)
SELECT
    CAST(vendor_id AS STRING) as vendor_id,
    TIMESTAMP_ADD(pickup_datetime, INTERVAL 1462 DAY) as pickup_datetime,  -- +4 years to 2025
    TIMESTAMP_ADD(dropoff_datetime, INTERVAL 1462 DAY) as dropoff_datetime,
    CAST(passenger_count AS INT64) as passenger_count,
    CAST(trip_distance AS FLOAT64) as trip_distance,
    CAST(pickup_location_id AS STRING) as pickup_location_id,
    CAST(dropoff_location_id AS STRING) as dropoff_location_id,
    CAST(rate_code AS STRING) as rate_code,
    CAST(payment_type AS STRING) as payment_type,
    CAST(fare_amount AS FLOAT64) as fare_amount,
    CAST(extra AS FLOAT64) as extra,
    CAST(mta_tax AS FLOAT64) as mta_tax,
    CAST(tip_amount AS FLOAT64) as tip_amount,
    CAST(tolls_amount AS FLOAT64) as tolls_amount,
    CAST(imp_surcharge AS FLOAT64) as imp_surcharge,
    CAST(airport_fee AS FLOAT64) as airport_fee,
    CAST(total_amount AS FLOAT64) as total_amount,
    CURRENT_TIMESTAMP() as processing_timestamp
FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2021`
WHERE DATE(pickup_datetime) BETWEEN '2021-11-01' AND '2021-11-26' 
    AND trip_distance > 0
    AND passenger_count > 0
    AND total_amount > 0
    AND pickup_location_id IS NOT NULL
    AND dropoff_location_id IS NOT NULL
ORDER BY RAND()
LIMIT 100000;  -- 100K trips để test

-- Verify
SELECT 
    COUNT(*) as total_trips,
    MIN(DATE(pickup_datetime)) as min_date,
    MAX(DATE(pickup_datetime)) as max_date
FROM `nyc-taxi-project-477115.streaming.processed_trips`;
