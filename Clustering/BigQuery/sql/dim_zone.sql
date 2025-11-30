CREATE OR REPLACE TABLE `nyc-taxi-project-479008.dimension.dim_zone` AS
SELECT
  zone_id,
  zone_name,
  borough,

  -- Polygon khu vực
  zone_geom,

  -- Tọa độ centroid (tâm polygon)
  ST_Y(ST_CENTROID(zone_geom)) AS centroid_lat,
  ST_X(ST_CENTROID(zone_geom)) AS centroid_lon

FROM `bigquery-public-data.new_york_taxi_trips.taxi_zone_geom`;
