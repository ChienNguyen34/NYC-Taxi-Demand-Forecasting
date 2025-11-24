-- bqml_scripts/train_model.sql
-- File này sẽ được gọi bởi Airflow SAU KHI dbt run thành công.

CREATE OR REPLACE MODEL `nyc-taxi-project-477115.ml_predictions.timeseries_hotspot_model`
OPTIONS(
    model_type='BOOSTED_TREE_REGRESSOR',
    input_label_cols=['total_pickups'],
    data_split_method='AUTO_SPLIT',
    max_iterations=50,
    min_rel_progress=0.01,
    early_stop=TRUE
)
AS
SELECT
    -- Target variable
    total_pickups,
    
    -- Time and location identifiers
    pickup_h3_id,
    EXTRACT(HOUR FROM timestamp_hour) AS hour_of_day,
    EXTRACT(DAYOFWEEK FROM timestamp_hour) AS day_of_week,
    
    -- Lag features
    pickups_1h_ago,
    pickups_24h_ago,
    pickups_1week_ago,
    
    -- Rolling average features
    avg_pickups_7h,
    avg_pickups_24h,
    
    -- Weather features
    avg_temp_celsius,
    total_precipitation_mm,
    had_rain,
    had_snow,
    
    -- Calendar features
    is_weekend,
    is_holiday,
    
    -- Trend features
    pickups_change_24h,
    
    -- Interaction features
    rain_during_rush_hour,
    
    -- Time features
    month,
    quarter,
    day_of_year
FROM
    `nyc-taxi-project-477115.facts.fct_hourly_features`
WHERE
    -- Loại bỏ rows có NULL trong lag features (first few rows)
    pickups_1h_ago IS NOT NULL
    AND pickups_24h_ago IS NOT NULL
    AND pickups_1week_ago IS NOT NULL