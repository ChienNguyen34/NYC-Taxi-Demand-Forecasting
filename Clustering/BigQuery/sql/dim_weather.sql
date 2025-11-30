CREATE OR REPLACE TABLE `nyc-taxi-project-479008.dimension.dim_weather` AS
WITH nyc_stations AS (
  -- Chọn các trạm thời tiết quanh khu vực New York (NY, USA)
  SELECT
    usaf,
    wban,
    name,
    country,
    state,
    lat,
    lon
  FROM `bigquery-public-data.noaa_gsod.stations`
  WHERE country = "US"
    AND state = "NY"
    -- Khung toạ độ xung quanh NYC (ước lượng)
    AND lat BETWEEN 40.3 AND 41.1
    AND lon BETWEEN -74.5 AND -73.3
),
nyc_weather AS (
  SELECT
    g.date,
    -- Nhiệt độ (đơn vị gốc của GSOD là °F)
    AVG(g.temp) AS avg_temp_f,
    MAX(g.max) AS max_temp_f,
    MIN(g.min) AS min_temp_f,

    -- Một số chỉ thị thời tiết
    MAX(g.fog)  AS fog_flag,   -- có sương mù trong ngày
    MAX(g.hail) AS hail_flag,  -- có mưa đá trong ngày

    -- Gió (nếu bạn cần thêm feature)
    AVG(g.gust) AS avg_gust_speed

  FROM `bigquery-public-data.noaa_gsod.gsod2021` AS g
  JOIN nyc_stations s
    ON g.stn  = s.usaf
   AND g.wban = s.wban
  GROUP BY g.date
)
SELECT
  -- khoá ngày để join với fact_trip
  date AS weather_date,
  EXTRACT(YEAR  FROM date) AS year,
  EXTRACT(MONTH FROM date) AS month,
  EXTRACT(DAY   FROM date) AS day,
  EXTRACT(DAYOFWEEK FROM date) AS day_of_week,

  -- feature thời tiết
  avg_temp_f,
  max_temp_f,
  min_temp_f,
  fog_flag,
  hail_flag,
  avg_gust_speed

FROM nyc_weather;
