CREATE OR REPLACE TABLE `nyc-taxi-project-479008.fact.fact_demand_zone_hour` AS
SELECT
  pickup_date,
  pickup_hour,
  pickup_zone_id      AS zone_id,
  pickup_zone_name,
  pickup_borough,

  COUNT(*)            AS trips_count,
  AVG(passenger_count) AS avg_passenger,
  AVG(trip_distance)   AS avg_distance,
  AVG(total_amount)    AS avg_total_amount,
  AVG(trip_duration_min) AS avg_duration_min

FROM `nyc-taxi-project-479008.fact.fact_trip`
GROUP BY
  pickup_date,
  pickup_hour,
  zone_id,
  pickup_zone_name,
  pickup_borough;
