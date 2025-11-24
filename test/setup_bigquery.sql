-- setup_bigquery.sql
-- Script tạo toàn bộ BigQuery infrastructure từ đầu
-- Chạy file này trong BigQuery Console hoặc bq CLI

-- ============================================================================
-- BƯỚC 1: TẠO DATASETS
-- ============================================================================

-- Dataset cho raw streaming data
CREATE SCHEMA IF NOT EXISTS `nyc-taxi-project-477115.raw_data`
OPTIONS(
  location='US',
  description='Raw data from external APIs (weather, etc.)'
);

-- Dataset cho streaming trips
CREATE SCHEMA IF NOT EXISTS `nyc-taxi-project-477115.streaming`
OPTIONS(
  location='US',
  description='Real-time streaming taxi trips data'
);

-- Dataset cho staging (dbt sẽ tạo tables)
CREATE SCHEMA IF NOT EXISTS `nyc-taxi-project-477115.staging`
OPTIONS(
  location='US',
  description='Staging layer - intermediate cleaned data'
);

-- Dataset cho dimensions (dbt sẽ tạo tables)
CREATE SCHEMA IF NOT EXISTS `nyc-taxi-project-477115.dimensions`
OPTIONS(
  location='US',
  description='Dimension tables (datetime, location, weather)'
);

-- Dataset cho facts (dbt sẽ tạo tables)
CREATE SCHEMA IF NOT EXISTS `nyc-taxi-project-477115.facts`
OPTIONS(
  location='US',
  description='Fact tables (trips, hourly features, demand)'
);

-- Dataset cho ML models
CREATE SCHEMA IF NOT EXISTS `nyc-taxi-project-477115.ml_models`
OPTIONS(
  location='US',
  description='BQML trained models'
);

-- Dataset cho ML predictions
CREATE SCHEMA IF NOT EXISTS `nyc-taxi-project-477115.ml_predictions`
OPTIONS(
  location='US',
  description='Output from BQML predictions'
);

-- ============================================================================
-- BƯỚC 2: TẠO STREAMING TABLES (CHO CLOUD FUNCTIONS)
-- ============================================================================

-- Table cho raw weather data từ OpenWeather API
CREATE TABLE IF NOT EXISTS `nyc-taxi-project-477115.raw_data.weather_api_data` (
    raw_json JSON,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(inserted_at)
OPTIONS(
  description='Raw weather data from OpenWeather API streaming',
  partition_expiration_days=90  -- Auto-delete partitions older than 90 days
);

-- Table cho streaming taxi trips
CREATE TABLE IF NOT EXISTS `nyc-taxi-project-477115.streaming.processed_trips` (
    vendor_id STRING,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count INT64,
    trip_distance FLOAT64,
    pickup_location_id STRING,
    dropoff_location_id STRING,
    fare_amount FLOAT64,
    total_amount FLOAT64,
    processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(pickup_datetime)
OPTIONS(
  description='Streaming taxi trips (2021 data shifted to 2025)',
  partition_expiration_days=90
);

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================

-- Check datasets created
SELECT 
  schema_name,
  location,
  creation_time as created_at
FROM nyc-taxi-project-477115.INFORMATION_SCHEMA.SCHEMATA
ORDER BY schema_name;

-- Check tables created
SELECT 
  table_schema,
  table_name,
  table_type,
  creation_time as created_at
FROM nyc-taxi-project-477115.INFORMATION_SCHEMA.TABLES
WHERE table_schema IN ('raw_data', 'streaming')
ORDER BY table_schema, table_name;

-- DONE! Giờ có thể chạy dbt để tạo staging/dimensions/facts tables
