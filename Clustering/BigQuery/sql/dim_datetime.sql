CREATE OR REPLACE TABLE `nyc-taxi-project-479008.dimension.dim_datetime` AS
WITH date_range AS (
  SELECT
    MIN(pickup_date) AS min_date,
    MAX(pickup_date) AS max_date
  FROM `nyc-taxi-project-479008.staging.tlc_yellow_trips_2021_stg`
),
calendar AS (
  SELECT
    day AS date_key,
    EXTRACT(YEAR FROM day) AS year,
    EXTRACT(QUARTER FROM day) AS quarter,
    EXTRACT(MONTH FROM day) AS month,
    EXTRACT(DAY FROM day) AS day,
    EXTRACT(DAYOFWEEK FROM day) AS day_of_week,      -- 1=Sunday
    FORMAT_DATE('%A', day) AS day_name,
    FORMAT_DATE('%Y-%m', day) AS year_month,
    CASE
      WHEN EXTRACT(DAYOFWEEK FROM day) IN (1, 7) THEN TRUE
      ELSE FALSE
    END AS is_weekend
  FROM date_range,
  UNNEST(GENERATE_DATE_ARRAY(min_date, max_date)) AS day
)
SELECT * FROM calendar;
