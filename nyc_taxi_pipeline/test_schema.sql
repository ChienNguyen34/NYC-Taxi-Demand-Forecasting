-- Test to see the schema of taxi_zone_geom
SELECT * FROM {{ source('public_data', 'taxi_zone_lookup') }}
LIMIT 5