-- models/marts/facts/fct_fare_prediction_training.sql
-- This model prepares the training data for the fare prediction BQML model.

WITH fct_trips AS (
    SELECT * FROM {{ ref('fct_trips') }}
),
dim_datetime AS (
    SELECT * FROM {{ ref('dim_datetime') }}
),
dim_weather AS (
    SELECT * FROM {{ ref('dim_weather') }}
),
agg_hourly_demand AS (
    SELECT
        pickup_h3_id,
        timestamp_hour,
        total_pickups AS historical_demand
    FROM {{ ref('agg_hourly_demand_h3') }}
)
SELECT
    -- Target variable
    trips.fare_amount * (1 + (RAND() - 0.5) * 2 * 0.1) AS fare_amount,

    -- Trip features
    trips.passenger_count,
    trips.trip_distance,
    trips.trip_duration_seconds,

    -- Datetime features
    EXTRACT(HOUR FROM trips.picked_up_at) AS hour_of_day,
    dt.day_of_week,
    dt.is_holiday,
    dt.is_weekend,

    -- Location features
    trips.pickup_h3_id,
    trips.dropoff_h3_id,

    -- Weather features (joined by date)
    weather.avg_temp_celsius,
    weather.total_precipitation_mm,
    weather.had_rain,
    weather.had_snow,

    -- Demand feature (joined by h3_id and hour)
    -- Use COALESCE to handle cases where there might not be demand data for a specific hour
    COALESCE(demand.historical_demand, 0) AS historical_demand

FROM
    fct_trips trips
LEFT JOIN
    dim_datetime dt ON trips.datetime_id = dt.date_id
LEFT JOIN
    dim_weather weather ON trips.weather_date = weather.weather_date
LEFT JOIN
    agg_hourly_demand demand ON trips.pickup_h3_id = demand.pickup_h3_id
    AND TIMESTAMP_TRUNC(trips.picked_up_at, HOUR) = demand.timestamp_hour

WHERE
    -- Filter out trips with no fare or negative fare, and trips that are too short/long
    trips.fare_amount > 0
    AND trips.trip_duration_seconds > 60
    AND trips.trip_duration_seconds < 7200 -- 2 hours
    AND trips.trip_distance > 0
    AND trips.trip_distance < 100 -- miles
