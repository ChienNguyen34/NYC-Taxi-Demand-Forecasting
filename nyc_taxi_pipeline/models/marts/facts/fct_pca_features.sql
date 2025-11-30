-- models/marts/facts/fct_pca_features.sql
-- PCA Feature Engineering for Demand Clustering Analysis
-- Generates 4 key metrics per H3 zone for Principal Component Analysis

{{ config(
    materialized='table',
    partition_by={
      "field": "created_at",
      "data_type": "date"
    }
) }}

WITH zone_trips AS (
    -- Feature 1: Total trips per zone (volume metric)
    SELECT
        pickup_h3_id,
        COUNT(*) AS total_trips,
        MIN(DATE(picked_up_at)) AS first_trip_date,
        MAX(DATE(picked_up_at)) AS last_trip_date
    FROM {{ ref('fct_trips') }}
    WHERE DATE(picked_up_at) >= '2025-01-01'
      AND DATE(picked_up_at) <= CURRENT_DATE()
    GROUP BY pickup_h3_id
),

zone_hourly_demand AS (
    -- Feature 2: Average hourly demand (intensity metric)
    SELECT
        pickup_h3_id,
        AVG(total_pickups) AS avg_hourly_demand,
        STDDEV(total_pickups) AS stddev_hourly_demand,
        MAX(total_pickups) AS peak_hourly_demand
    FROM {{ ref('fct_hourly_features') }}
    WHERE DATE(timestamp_hour) >= '2025-01-01'
      AND DATE(timestamp_hour) <= CURRENT_DATE()
    GROUP BY pickup_h3_id
),

zone_weekend_pattern AS (
    -- Feature 4: Weekend vs Weekday behavior ratio
    SELECT
        pickup_h3_id,
        AVG(CASE WHEN is_weekend THEN total_pickups ELSE NULL END) AS weekend_avg_demand,
        AVG(CASE WHEN NOT is_weekend THEN total_pickups ELSE NULL END) AS weekday_avg_demand,
        SAFE_DIVIDE(
            AVG(CASE WHEN is_weekend THEN total_pickups ELSE NULL END),
            AVG(CASE WHEN NOT is_weekend THEN total_pickups ELSE NULL END)
        ) AS weekend_ratio,
        -- Additional temporal patterns
        COUNT(DISTINCT CASE WHEN is_weekend THEN DATE(timestamp_hour) END) AS weekend_days_active,
        COUNT(DISTINCT CASE WHEN NOT is_weekend THEN DATE(timestamp_hour) END) AS weekday_days_active
    FROM {{ ref('fct_hourly_features') }}
    WHERE DATE(timestamp_hour) >= '2025-01-01'
      AND DATE(timestamp_hour) <= CURRENT_DATE()
    GROUP BY pickup_h3_id
),

zone_time_patterns AS (
    -- Additional: Peak hours analysis
    SELECT
        pickup_h3_id,
        AVG(CASE 
            WHEN EXTRACT(HOUR FROM timestamp_hour) BETWEEN 7 AND 9 
            THEN total_pickups 
            ELSE NULL 
        END) AS morning_rush_demand,
        AVG(CASE 
            WHEN EXTRACT(HOUR FROM timestamp_hour) BETWEEN 17 AND 19 
            THEN total_pickups 
            ELSE NULL 
        END) AS evening_rush_demand,
        AVG(CASE 
            WHEN EXTRACT(HOUR FROM timestamp_hour) BETWEEN 22 AND 4 
            THEN total_pickups 
            ELSE NULL 
        END) AS night_demand
    FROM {{ ref('fct_hourly_features') }}
    WHERE DATE(timestamp_hour) >= '2025-01-01'
      AND DATE(timestamp_hour) <= CURRENT_DATE()
    GROUP BY pickup_h3_id
)

SELECT
    -- Zone identifiers
    loc.h3_id AS pickup_h3_id,
    loc.zone_name,
    loc.borough,
    loc.h3_centroid_latitude AS latitude,
    loc.h3_centroid_longitude AS longitude,
    
    -- === 4 CORE PCA FEATURES ===
    
    -- Feature 1: Total trips (volume)
    COALESCE(zt.total_trips, 0) AS total_trips,
    
    -- Feature 2: Average hourly demand (intensity)
    COALESCE(zhd.avg_hourly_demand, 0.0) AS avg_hourly_demand,
    
    -- Feature 3: Trips per km² (density)
    -- H3 resolution 8 has area ~0.737 km²
    COALESCE(zt.total_trips / 0.737, 0.0) AS trips_per_km2,
    
    -- Feature 4: Weekend ratio (behavior pattern)
    COALESCE(zwp.weekend_ratio, 1.0) AS weekend_ratio,
    
    -- === ADDITIONAL CONTEXT FEATURES ===
    
    -- Demand variability
    COALESCE(zhd.stddev_hourly_demand, 0.0) AS stddev_hourly_demand,
    COALESCE(zhd.peak_hourly_demand, 0) AS peak_hourly_demand,
    
    -- Weekend/Weekday breakdown
    COALESCE(zwp.weekend_avg_demand, 0.0) AS weekend_avg_demand,
    COALESCE(zwp.weekday_avg_demand, 0.0) AS weekday_avg_demand,
    COALESCE(zwp.weekend_days_active, 0) AS weekend_days_active,
    COALESCE(zwp.weekday_days_active, 0) AS weekday_days_active,
    
    -- Time-of-day patterns
    COALESCE(ztp.morning_rush_demand, 0.0) AS morning_rush_demand,
    COALESCE(ztp.evening_rush_demand, 0.0) AS evening_rush_demand,
    COALESCE(ztp.night_demand, 0.0) AS night_demand,
    
    -- Rush hour ratio
    SAFE_DIVIDE(
        COALESCE(ztp.morning_rush_demand, 0) + COALESCE(ztp.evening_rush_demand, 0),
        COALESCE(zhd.avg_hourly_demand, 1)
    ) AS rush_hour_ratio,
    
    -- Data quality metrics
    zt.first_trip_date,
    zt.last_trip_date,
    DATE_DIFF(zt.last_trip_date, zt.first_trip_date, DAY) AS days_active,
    
    -- Timestamp
    CURRENT_DATE() AS created_at

FROM {{ ref('dim_location') }} loc
LEFT JOIN zone_trips zt ON loc.h3_id = zt.pickup_h3_id
LEFT JOIN zone_hourly_demand zhd ON loc.h3_id = zhd.pickup_h3_id
LEFT JOIN zone_weekend_pattern zwp ON loc.h3_id = zwp.pickup_h3_id
LEFT JOIN zone_time_patterns ztp ON loc.h3_id = ztp.pickup_h3_id

-- Only include zones with actual trip data
WHERE zt.total_trips IS NOT NULL
  AND zt.total_trips > 0
