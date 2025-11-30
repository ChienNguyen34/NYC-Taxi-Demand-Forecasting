CREATE OR REPLACE TABLE `nyc-taxi-project-479008.staging.tlc_yellow_trips_2021_stg` AS
SELECT
  -- Thông tin hãng xe
  vendor_id,

  -- Thời gian
  pickup_datetime AS pickup_at,
  dropoff_datetime AS dropoff_at,
  DATE(pickup_datetime) AS pickup_date,
  EXTRACT(HOUR FROM pickup_datetime) AS pickup_hour,
  EXTRACT(DAYOFWEEK FROM pickup_datetime) AS pickup_day_of_week,

  -- Thời lượng chuyến đi
  TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, MINUTE) AS trip_duration_min,

  -- Hành khách
  passenger_count,

  -- Khoảng cách / Giá
  trip_distance,
  fare_amount,
  total_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  imp_surcharge,
  airport_fee,

  -- Zone
  pickup_location_id,
  dropoff_location_id

FROM `nyc-taxi-project-479008.raw_data_source.tlc_yellow_trips_2021_raw`
WHERE trip_distance >= 0
  AND fare_amount >= 0
  AND pickup_datetime IS NOT NULL
  AND dropoff_datetime IS NOT NULL
  AND passenger_count IS NOT NULL
  AND passenger_count > 0;     -- chỉ lấy các chuyến có chở khách
