-- Create streaming table with ALL fields from TLC dataset
CREATE TABLE IF NOT EXISTS `nyc-taxi-project-477115.streaming.processed_trips` (
    vendor_id STRING,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count INT64,
    trip_distance FLOAT64,
    pickup_location_id STRING,
    dropoff_location_id STRING,
    rate_code STRING,
    payment_type STRING,
    fare_amount FLOAT64,
    extra FLOAT64,
    mta_tax FLOAT64,
    tip_amount FLOAT64,
    tolls_amount FLOAT64,
    imp_surcharge FLOAT64,
    airport_fee FLOAT64,
    total_amount FLOAT64,
    processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(pickup_datetime)
OPTIONS(
  description='Streaming taxi trips with full TLC fields (2021 data shifted to 2025)',
  partition_expiration_days=90
);
