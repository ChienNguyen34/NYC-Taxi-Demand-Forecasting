CREATE OR REPLACE TABLE `nyc-taxi-project-479008.fact.ml_dataset` AS
WITH zone_stats AS (
  -- Tổng hợp các chỉ số nhu cầu theo từng zone
  SELECT
    zone_id,
    pickup_zone_name,
    SUM(trips_count) AS total_trips,                      -- (1) Tổng số chuyến
    AVG(trips_count) AS avg_hourly_demand,               -- (2) Nhu cầu trung bình theo giờ

    -- (4) Nhu cầu trung bình cuối tuần
    AVG(
      CASE
        WHEN EXTRACT(DAYOFWEEK FROM pickup_date) IN (1, 7) THEN trips_count
        ELSE NULL
      END
    ) AS weekend_avg_trips,

    -- (4) Nhu cầu trung bình ngày thường
    AVG(
      CASE
        WHEN EXTRACT(DAYOFWEEK FROM pickup_date) NOT IN (1, 7) THEN trips_count
        ELSE NULL
      END
    ) AS weekday_avg_trips
  FROM `nyc-taxi-project-479008.fact.fact_demand_zone_hour`
  GROUP BY
    zone_id,
    pickup_zone_name
),

zone_area AS (
  -- Tính diện tích từng zone (km2) từ bảng polygon
  SELECT
    zone_id,
    ST_AREA(zone_geom) / 1e6 AS area_km2   -- m² → km²
  FROM `bigquery-public-data.new_york_taxi_trips.taxi_zone_geom`
)

SELECT
  zs.zone_id,
  zs.pickup_zone_name,

  -- (1) Tổng số chuyến
  zs.total_trips,

  -- (2) Nhu cầu trung bình theo giờ
  zs.avg_hourly_demand,

  -- (3) Mật độ nhu cầu: số chuyến / km2
  SAFE_DIVIDE(zs.total_trips, za.area_km2) AS trips_per_km2,

  -- (4) Tỉ lệ cuối tuần / ngày thường
  SAFE_DIVIDE(zs.weekend_avg_trips, zs.weekday_avg_trips) AS weekend_ratio,

  -- Tham khảo thêm
  za.area_km2,
  zs.weekend_avg_trips,
  zs.weekday_avg_trips
FROM zone_stats zs
LEFT JOIN zone_area za
  ON zs.zone_id = za.zone_id;
