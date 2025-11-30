-- test/populate_more_trips.sql
-- Insert thêm 100K trips từ Sept-Nov 2021 (shift sang 2025) để có đủ data train

INSERT INTO `nyc-taxi-project-477115.streaming.processed_trips` (
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    pickup_location_id,
    dropoff_location_id,
    fare_amount,
    total_amount,
    processing_timestamp
)
SELECT
    -- Vendor ID
    CAST(vendor_id AS STRING) as vendor_id,
    
    -- Shift timestamps from 2021 to 2025 (+1461 days = 4 years)
    TIMESTAMP_ADD(CAST(pickup_datetime AS TIMESTAMP), INTERVAL 1461 DAY) as pickup_datetime,
    TIMESTAMP_ADD(CAST(dropoff_datetime AS TIMESTAMP), INTERVAL 1461 DAY) as dropoff_datetime,
    
    -- Trip details
    CAST(passenger_count AS INT64) as passenger_count,
    trip_distance,
    
    -- Location IDs (already STRING in source)
    pickup_location_id,
    dropoff_location_id,
    
    -- Amounts
    fare_amount,
    total_amount,
    
    -- Processing timestamp (current time)
    CURRENT_TIMESTAMP() as processing_timestamp

FROM
    `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2021`

WHERE
    -- Lấy data từ Sept 1 - Nov 30, 2021 (3 tháng)
    DATE(pickup_datetime) BETWEEN '2021-01-01' AND '2021-12-06'
    
    -- Filter data quality
    AND pickup_datetime IS NOT NULL
    AND dropoff_datetime IS NOT NULL
    AND fare_amount > 0
    AND trip_distance > 0
    AND passenger_count > 0
    AND total_amount > 0
    AND pickup_location_id IS NOT NULL
    AND dropoff_location_id IS NOT NULL

-- Lấy random 100K trips
ORDER BY RAND()
LIMIT 100000
