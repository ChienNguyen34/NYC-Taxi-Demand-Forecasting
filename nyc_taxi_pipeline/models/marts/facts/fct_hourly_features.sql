-- models/marts/facts/fct_hourly_features.sql
-- CHỨA TOÀN BỘ LOGIC FEATURE ENGINEERING (LAGS, AVGS) BẠN ĐÃ VIẾT

WITH enriched_data AS (
  SELECT 
    agg.*,
    dim_dt.month,
    dim_dt.quarter,
    dim_dt.day_of_year
  FROM `facts.agg_hourly_demand_h3` agg
  LEFT JOIN `dimensions.dim_datetime` dim_dt
    ON DATE(agg.timestamp_hour) = dim_dt.full_date
),

lag_features AS (
    SELECT
        *,
        -- Lag features (để model hiểu patterns từ quá khứ)
        LAG(total_pickups, 1) OVER (
            PARTITION BY pickup_h3_id 
            ORDER BY timestamp_hour
        ) as pickups_1h_ago,
        
        LAG(total_pickups, 24) OVER (
            PARTITION BY pickup_h3_id 
            ORDER BY timestamp_hour  
        ) as pickups_24h_ago,
        
        LAG(total_pickups, 168) OVER (
            PARTITION BY pickup_h3_id 
            ORDER BY timestamp_hour  
        ) as pickups_1week_ago,
        
        -- Rolling averages (smooth out noise)
        AVG(total_pickups) OVER (
            PARTITION BY pickup_h3_id 
            ORDER BY timestamp_hour 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as avg_pickups_7h,
        
        AVG(total_pickups) OVER (
            PARTITION BY pickup_h3_id 
            ORDER BY timestamp_hour 
            ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
        ) as avg_pickups_24h
    FROM enriched_data
)

SELECT 
    -- Target
    l.total_pickups,
    
    -- Location
    l.pickup_h3_id,

    -- Timestamp (dùng cho Time Series model)
    l.timestamp_hour,
    
    -- Time features
    l.hour_of_day,
    l.day_of_week,
    l.month,
    l.quarter,
    l.day_of_year,
    
    -- Weather features
    l.avg_temp_celsius, -- (FIXED: Tên cột đúng là avg_temp_c)
    l.total_precipitation_mm,
    l.had_rain, -- (FIXED: Sửa tên cột)
    l.had_snow,
    
    -- Calendar features
    l.is_weekend,
    l.is_holiday,
    
    -- Lag features
    l.pickups_1h_ago,
    l.pickups_24h_ago,
    l.pickups_1week_ago,
    
    -- Rolling averages
    l.avg_pickups_7h,
    l.avg_pickups_24h,
    
    -- Trend features
    l.total_pickups - l.pickups_24h_ago as pickups_change_24h,
    
    -- Interaction features
    CASE 
        WHEN l.had_rain AND l.hour_of_day BETWEEN 7 AND 9 THEN 1
        WHEN l.had_rain AND l.hour_of_day BETWEEN 17 AND 19 THEN 1  
        ELSE 0 
    END as rain_during_rush_hour
    
FROM lag_features l

-- Chỉ lấy data có đủ lag features (bỏ 168 giờ = 1 tuần đầu)
WHERE l.timestamp_hour >= TIMESTAMP_ADD(
    (SELECT MIN(timestamp_hour) FROM enriched_data),
    INTERVAL 168 HOUR
)