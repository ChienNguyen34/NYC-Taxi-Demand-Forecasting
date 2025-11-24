-- populate_streaming_trips.sql
-- Script INSERT manual data vào streaming.processed_trips để test pipeline
-- Lấy 10,000 trips từ public dataset 2021, shift time sang 2025

INSERT INTO `nyc-taxi-project-477115.streaming.processed_trips` 
(vendor_id, pickup_datetime, dropoff_datetime, passenger_count, trip_distance, 
 pickup_location_id, dropoff_location_id, fare_amount, total_amount, processing_timestamp)
SELECT
    CAST(vendor_id AS STRING) as vendor_id,
    TIMESTAMP_ADD(pickup_datetime, INTERVAL 1461 DAY) as pickup_datetime,  -- +4 years
    TIMESTAMP_ADD(dropoff_datetime, INTERVAL 1461 DAY) as dropoff_datetime,
    CAST(passenger_count AS INT64) as passenger_count,
    CAST(trip_distance AS FLOAT64) as trip_distance,
    CAST(pickup_location_id AS STRING) as pickup_location_id,
    CAST(dropoff_location_id AS STRING) as dropoff_location_id,
    CAST(fare_amount AS FLOAT64) as fare_amount,
    CAST(total_amount AS FLOAT64) as total_amount,
    CURRENT_TIMESTAMP() as processing_timestamp
FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2021`
WHERE DATE(pickup_datetime) BETWEEN '2021-11-01' AND '2021-11-23'  -- 23 ngày data
    AND trip_distance > 0
    AND passenger_count > 0
    AND total_amount > 0
    AND pickup_location_id IS NOT NULL
    AND dropoff_location_id IS NOT NULL
ORDER BY RAND()
LIMIT 10000;  -- 10K trips để test

-- Verify
SELECT 
    COUNT(*) as total_trips,
    MIN(DATE(pickup_datetime)) as min_date,
    MAX(DATE(pickup_datetime)) as max_date
FROM `nyc-taxi-project-477115.streaming.processed_trips`;
