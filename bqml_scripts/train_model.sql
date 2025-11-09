-- bqml_scripts/train_model.sql
-- File này sẽ được gọi bởi Airflow SAU KHI dbt run thành công.

CREATE OR REPLACE MODEL `nyc-taxi-project-477115.ml_predictions.timeseries_hotspot_model`
OPTIONS(
    model_type='ARIMA_PLUS',
    time_series_timestamp_col='timestamp_hour',
    time_series_data_col='total_pickups',
    time_series_id_col='pickup_h3_id',
    data_frequency='HOURLY',
    holiday_region='US'
)
AS
SELECT
    timestamp_hour,    -- time_series_timestamp_col
    pickup_h3_id,     -- time_series_id_col  
    total_pickups     -- time_series_data_col
    -- ARIMA_PLUS chỉ cho phép đúng 3 cột này, không thể thêm external regressors
FROM
    `nyc-taxi-project-477115.facts.fct_hourly_features`