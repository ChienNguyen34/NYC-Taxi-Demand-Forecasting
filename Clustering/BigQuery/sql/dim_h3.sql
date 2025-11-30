CREATE OR REPLACE TABLE `nyc-taxi-project-479008.dimension.dim_h3_zone` AS
SELECT
  z.zone_id,
  z.zone_name,
  z.borough,
  h3 AS h3_index,
  8  AS h3_resolution
FROM `nyc-taxi-project-479008.dimension.dim_zone` AS z
CROSS JOIN UNNEST(
  bqcarto.h3.ST_ASH3_POLYFILL(z.zone_geom, 8)
) AS h3;
