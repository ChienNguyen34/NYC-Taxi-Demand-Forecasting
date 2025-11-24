-- test/populate_weather_data.sql
-- Insert fake weather data cho Sept-Nov 2025 vào staging.fake_weather_2025
-- Dùng NOAA 2021 data + shift 4 năm

CREATE OR REPLACE TABLE `nyc-taxi-project-477115.staging.fake_weather_2025` AS
SELECT
    -- Convert DATE to DATE (shift 4 years)
    DATE_ADD(CAST(date AS DATE), INTERVAL 4 YEAR) as observation_date,
    
    -- Temperature (Fahrenheit to Celsius, aggregate by day across all stations)
    ROUND(AVG((CAST(temp AS NUMERIC) - 32) * 5 / 9), 2) as avg_temp_celsius,
    ROUND(MAX((CAST(max AS NUMERIC) - 32) * 5 / 9), 2) as max_temp_celsius,
    ROUND(MIN((CAST(min AS NUMERIC) - 32) * 5 / 9), 2) as min_temp_celsius,
    
    -- Precipitation (inches to mm: 1 inch = 25.4 mm)
    ROUND(AVG(CAST(prcp AS NUMERIC) * 25.4), 2) as precipitation_mm,
    
    -- Weather flags (aggregate across stations using LOGICAL_OR)
    LOGICAL_OR(CAST(rain_drizzle AS INT64) = 1) as is_rainy,
    LOGICAL_OR(CAST(snow_ice_pellets AS INT64) = 1) as is_snowy,
    LOGICAL_OR(CAST(fog AS INT64) = 1) as is_foggy

FROM
    `bigquery-public-data.noaa_gsod.gsod2021`

WHERE
    -- NYC weather stations
    stn IN ('725030', '744860', '725053')
    
    -- Sept 1 - Nov 30, 2021 (will be shifted to 2025)
    AND DATE(date) BETWEEN '2021-09-01' AND '2021-11-30'
    
    -- Data quality filters
    AND temp IS NOT NULL
    AND prcp IS NOT NULL

GROUP BY
    observation_date

ORDER BY
    observation_date
