# Hướng dẫn Huấn luyện Model Dự đoán Giá cước

Tài liệu này hướng dẫn bạn cách triển khai và chạy pipeline để huấn luyện mô hình dự đoán giá cước (`fare_estimation_model`) bằng dbt và BigQuery ML.

## 1. Tổng quan

Pipeline này bao gồm các bước sau:
1.  **Chuẩn bị dữ liệu (dbt):** Một model dbt mới có tên `fct_fare_prediction_training` sẽ được chạy để tạo ra một bảng dữ liệu sạch, đầy đủ feature cho việc huấn luyện.
2.  **Huấn luyện Model (BQML):** Một script SQL sẽ được thực thi để huấn luyện mô hình `BOOSTED_TREE_REGRESSOR` trên dữ liệu đã được chuẩn bị.
3.  **Tích hợp (Airflow):** Toàn bộ luồng công việc này được điều phối bởi Airflow DAG `nyc-taxi-orchestrator`.

## 2. Yêu cầu tiên quyết

- Đảm bảo rằng bạn đã cấu hình môi trường GCP và `gcloud` CLI của mình.
- Đảm bảo rằng các dbt model cơ bản (staging, dimensions, facts) đã được chạy thành công ít nhất một lần.

## 3. Các bước thực thi (Local)

Để chạy pipeline này theo cách thủ công trên môi trường local của bạn.

### Bước 3.1: Chạy dbt model để chuẩn bị dữ liệu

Lệnh này sẽ build bảng `fct_fare_prediction_training` và tất cả các bảng mà nó phụ thuộc.

```bash
# Chạy từ thư mục gốc của dự án
dbt run --select fct_fare_prediction_training --project-dir nyc_taxi_pipeline --profiles-dir nyc_taxi_pipeline
```

*Lưu ý: Nếu bạn gặp lỗi `UnicodeDecodeError` trên Windows, hãy đảm bảo terminal của bạn được cấu hình để sử dụng UTF-8, hoặc chạy các lệnh trong môi trường Linux (WSL, Docker).*

### Bước 3.2: Chạy script huấn luyện BQML

Sau khi dữ liệu đã sẵn sàng, bạn có thể chạy script SQL để huấn luyện mô hình.

1.  **Thay thế Project ID:** Mở file `bqml_scripts/train_fare_model.sql` và thay thế `{{ var.gcp_project_id }}` bằng Project ID thực tế của bạn.

2.  **Chạy lệnh `bq`:**
    ```bash
    # Đảm bảo bạn đã đăng nhập gcloud
    gcloud auth application-default login

    # Chạy query từ file
    bq query --use_legacy_sql=false < bqml_scripts/train_fare_model.sql
    ```

## 4. Tích hợp Airflow

- **Tự động:** Nếu bạn đã triển khai Airflow, DAG `nyc-taxi-orchestrator` đã được cập nhật để tự động chạy pipeline này hàng ngày.
- **Thứ tự:** Task `bqml_train_fare_model` sẽ được kích hoạt sau khi `dbt_test` hoàn thành.

---
**Hoàn tất!** Mô hình `ml_models.fare_estimation_model` của bạn sẽ được huấn luyện và sẵn sàng để sử dụng cho việc dự đoán giá cước.
