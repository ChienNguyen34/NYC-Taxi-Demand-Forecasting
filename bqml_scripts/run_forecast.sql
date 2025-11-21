-- bqml_scripts/run_forecast.sql
-- File này sẽ được gọi bởi Airflow SAU KHI train model thành công.

-- Xóa bảng cũ (nếu có) và tạo bảng dự báo mới
CREATE OR REPLACE TABLE `nyc-taxi-project-478411.ml_predictions.hourly_demand_forecast`
AS
SELECT *
FROM
  ML.FORECAST(
    MODEL `nyc-taxi-project-478411.ml_predictions.timeseries_hotspot_model`, -- Model từ ml_models dataset
    STRUCT(
        24 AS horizon, -- Dự báo 24 giờ tới
        0.8 AS confidence_level
    )
  );