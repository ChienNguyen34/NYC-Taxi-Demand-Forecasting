-- bqml_scripts/run_forecast.sql
-- File này sẽ được gọi bởi Airflow SAU KHI train model thành công.

-- Xóa bảng cũ (nếu có) và tạo bảng dự báo mới
CREATE OR REPLACE TABLE `nyc-taxi-project-477115.ml_predictions.hourly_demand_forecast`
AS
SELECT 
    f.timestamp_hour,
    f.pickup_h3_id,
    f.total_pickups AS actual_pickups,
    p.predicted_total_pickups,
    p.predicted_total_pickups_lower_bound,
    p.predicted_total_pickups_upper_bound,
    -- Include key features for analysis
    f.avg_temp_celsius,
    f.had_rain,
    f.is_weekend,
    f.is_holiday
FROM
    `nyc-taxi-project-477115.facts.fct_hourly_features` f
INNER JOIN
    ML.PREDICT(
        MODEL `nyc-taxi-project-477115.ml_predictions.timeseries_hotspot_model`,
        (
            SELECT
                pickup_h3_id,
                EXTRACT(HOUR FROM timestamp_hour) AS hour_of_day,
                EXTRACT(DAYOFWEEK FROM timestamp_hour) AS day_of_week,
                pickups_1h_ago,
                pickups_24h_ago,
                pickups_1week_ago,
                avg_pickups_7h,
                avg_pickups_24h,
                avg_temp_celsius,
                total_precipitation_mm,
                had_rain,
                had_snow,
                is_weekend,
                is_holiday,
                pickups_change_24h,
                rain_during_rush_hour,
                month,
                quarter,
                day_of_year
            FROM
                `nyc-taxi-project-477115.facts.fct_hourly_features`
            WHERE
                -- Predict cho 24 giờ gần nhất (hoặc data mới nhất)
                timestamp_hour >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
                AND pickups_1h_ago IS NOT NULL
                AND pickups_24h_ago IS NOT NULL
                AND pickups_1week_ago IS NOT NULL
        )
    ) p
ON f.pickup_h3_id = p.pickup_h3_id
    AND EXTRACT(HOUR FROM f.timestamp_hour) = p.hour_of_day
    AND EXTRACT(DAYOFWEEK FROM f.timestamp_hour) = p.day_of_week;