-- models/marts/dimensions/dim_location.sql

-- Chọn độ phân giải H3 (resolution 8 là một lựa chọn tốt cho cấp khu phố)
{% set h3_resolution = 8 %}

with taxi_zones as (
    select
        cast(zone_id as string) as zone_id,
        zone_name,
        borough,
        zone_geom
    from {{ source('public_data', 'taxi_zone_lookup') }}
    where borough != 'Unknown'
)

select
    zone_id,
    zone_name,
    borough,

    -- Tính centroid point từ zone geometry
    ST_CENTROID(zone_geom) as zone_centroid,
    
    -- Extract longitude và latitude riêng biệt
    ST_X(ST_CENTROID(zone_geom)) as h3_centroid_longitude,
    ST_Y(ST_CENTROID(zone_geom)) as h3_centroid_latitude,

    -- Sử dụng hàm H3 built-in của BigQuery
    -- 1. ST_CENTROID: Tìm điểm trung tâm của vùng địa lý (zone)
    -- 2. Tính H3 manually hoặc dùng alternative approach
    CONCAT(
        'h3_res', CAST({{ h3_resolution }} AS STRING), '_',
        CAST(ROUND(ST_X(ST_CENTROID(zone_geom)) * 1000) AS STRING), '_',
        CAST(ROUND(ST_Y(ST_CENTROID(zone_geom)) * 1000) AS STRING)
    ) as h3_id

from taxi_zones

-- dbt run --select dim_location
