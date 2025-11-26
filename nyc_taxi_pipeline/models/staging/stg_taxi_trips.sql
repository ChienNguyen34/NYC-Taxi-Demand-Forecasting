-- models/staging/stg_taxi_trips.sql
-- Combines historical data (2021 shifted to 2025) with real-time streaming data

WITH historical_trips AS (
    SELECT
        -- IDs (Tất cả đều là STRING trong nguồn)
        cast(vendor_id as string) as vendor_id,
        
        -- Timestamps - Shift to 2025 full year (1462 days from 2021)
        TIMESTAMP_ADD(CAST(pickup_datetime AS TIMESTAMP), INTERVAL 1462 DAY) as picked_up_at,
        TIMESTAMP_ADD(CAST(dropoff_datetime AS TIMESTAMP), INTERVAL 1462 DAY) as dropped_off_at,
        
        -- Trip info
        cast(passenger_count as int64) as passenger_count,
        cast(trip_distance as numeric) as trip_distance,
        
        -- Location info (Là STRING trong nguồn)
        cast(pickup_location_id as string) as pickup_location_id,
        cast(dropoff_location_id as string) as dropoff_location_id,
        
        -- Payment info (Là STRING trong nguồn)
        cast(rate_code as string) as rate_code_id,
        cast(payment_type as string) as payment_type_id,
        
        -- Numeric payment info
        cast(fare_amount as numeric) as fare_amount,
        cast(extra as numeric) as extra_amount,
        cast(mta_tax as numeric) as mta_tax,
        cast(tip_amount as numeric) as tip_amount,
        cast(tolls_amount as numeric) as tolls_amount,
        cast(imp_surcharge as numeric) as improvement_surcharge,
        cast(airport_fee as numeric) as airport_fee,
        cast(total_amount as numeric) as total_amount

    FROM {{ source('public_data', 'tlc_yellow_trips_2021') }}

    WHERE
        trip_distance > 0
        AND passenger_count > 0
        AND total_amount > 0
        AND pickup_datetime >= '2021-01-01'  -- Start from beginning of year
        AND pickup_datetime < '2021-11-24'   -- Keep streaming cutoff date
        AND pickup_location_id IS NOT NULL
        AND dropoff_location_id IS NOT NULL
        AND pickup_location_id != ''
        AND dropoff_location_id != ''
),

streaming_trips AS (
    SELECT
        CAST(vendor_id AS STRING) AS vendor_id,
        pickup_datetime AS picked_up_at,
        dropoff_datetime AS dropped_off_at,
        CAST(passenger_count AS INT64) AS passenger_count,
        CAST(trip_distance AS NUMERIC) AS trip_distance,
        CAST(pickup_location_id AS STRING) AS pickup_location_id,
        CAST(dropoff_location_id AS STRING) AS dropoff_location_id,
        CAST(rate_code AS STRING) AS rate_code_id,
        CAST(payment_type AS STRING) AS payment_type_id,
        CAST(fare_amount AS NUMERIC) AS fare_amount,
        CAST(extra AS NUMERIC) AS extra_amount,
        CAST(mta_tax AS NUMERIC) AS mta_tax,
        CAST(tip_amount AS NUMERIC) AS tip_amount,
        CAST(tolls_amount AS NUMERIC) AS tolls_amount,
        CAST(imp_surcharge AS NUMERIC) AS improvement_surcharge,
        CAST(airport_fee AS NUMERIC) AS airport_fee,
        CAST(total_amount AS NUMERIC) AS total_amount
        
    FROM {{ source('streaming_data', 'processed_trips') }}
    
    WHERE
        trip_distance > 0
        AND passenger_count > 0
        AND total_amount > 0
        AND pickup_datetime >= TIMESTAMP('2025-11-24')  -- Only streaming data after historical cutoff
        AND pickup_location_id IS NOT NULL
        AND dropoff_location_id IS NOT NULL
)

-- Combine both sources
SELECT * FROM historical_trips
UNION ALL
SELECT * FROM streaming_trips

-- dbt run --select stg_taxi_trips
