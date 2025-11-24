-- Test query to validate syntax
SELECT
    timestamp_hour,
    pickup_h3_id,
    total_pickups,
    avg_temp_celsius,
    total_precipitation_mm,
    had_rain,
    had_snow,
    is_weekend,
    is_holiday,
    day_of_week,
    hour_of_day
FROM
    `nyc-taxi-project-477115.staging_layer.agg_hourly_demand_h3`
LIMIT 5