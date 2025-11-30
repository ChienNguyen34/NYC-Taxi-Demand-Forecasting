CREATE OR REPLACE TABLE `nyc-taxi-project-479008.raw_data_source.tlc_yellow_trips_2021_raw`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY vendor_id
AS
SELECT *
FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2021`;
