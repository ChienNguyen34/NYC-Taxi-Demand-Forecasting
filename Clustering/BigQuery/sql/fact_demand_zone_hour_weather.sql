CREATE OR REPLACE TABLE `nyc-taxi-project-479008.fact.fact_demand_zone_hour_weather` AS
SELECT
  f.*,
  w.avg_temp_f,
  w.max_temp_f,
  w.min_temp_f,
  w.fog_flag,
  w.hail_flag,
  w.avg_gust_speed
FROM `nyc-taxi-project-479008.fact.fact_demand_zone_hour` AS f
LEFT JOIN `nyc-taxi-project-479008.dimension.dim_weather` AS w
  ON f.pickup_date = w.weather_date;
