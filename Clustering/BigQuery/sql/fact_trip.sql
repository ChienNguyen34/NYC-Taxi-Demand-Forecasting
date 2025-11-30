CREATE OR REPLACE TABLE `nyc-taxi-project-479008.fact.fact_trip` AS
SELECT
  -- Key / thời gian
  stg.pickup_at,
  stg.dropoff_at,
  stg.pickup_date,
  stg.pickup_hour,
  stg.pickup_day_of_week,

  -- Thông tin chuyến đi
  stg.vendor_id,
  stg.passenger_count,
  stg.trip_distance,
  stg.trip_duration_min,
  stg.fare_amount,
  stg.total_amount,
  stg.extra,
  stg.mta_tax,
  stg.tip_amount,
  stg.tolls_amount,
  stg.imp_surcharge,
  stg.airport_fee,

  -- Zone pickup
  stg.pickup_location_id              AS pickup_zone_id,
  dz_pickup.zone_name                 AS pickup_zone_name,
  dz_pickup.borough                   AS pickup_borough,

  -- Zone dropoff
  stg.dropoff_location_id             AS dropoff_zone_id,
  dz_dropoff.zone_name                AS dropoff_zone_name,
  dz_dropoff.borough                  AS dropoff_borough

FROM `nyc-taxi-project-479008.staging.tlc_yellow_trips_2021_stg` AS stg
LEFT JOIN `nyc-taxi-project-479008.dimension.dim_zone` AS dz_pickup
  ON stg.pickup_location_id = dz_pickup.zone_id
LEFT JOIN `nyc-taxi-project-479008.dimension.dim_zone` AS dz_dropoff
  ON stg.dropoff_location_id = dz_dropoff.zone_id;
