# Hướng dẫn triển khai Pipeline Streaming Dữ liệu Thời tiết

Tài liệu này hướng dẫn bạn từng bước để cấu hình và triển khai pipeline streaming dữ liệu thời tiết trên Google Cloud Platform (GCP) bằng các tài nguyên đã được tạo.

## 0. Yêu cầu tiên quyết

Trước khi bắt đầu, hãy đảm bảo bạn đã chuẩn bị sẵn sàng các mục sau:

1.  **Cài đặt `gcloud` CLI:** Nếu chưa có, hãy [cài đặt Google Cloud CLI](https://cloud.google.com/sdk/docs/install) và đăng nhập bằng lệnh `gcloud auth login`.
2.  **Tạo Project GCP:** Bạn cần có một project trên GCP với tính năng thanh toán (billing) đã được kích hoạt.
3.  **Lấy API Key:** Đăng ký tài khoản miễn phí trên [OpenWeatherMap](https://openweathermap.org/api) và lấy API Key của bạn.
4.  **Kích hoạt các API cần thiết:** Chạy lệnh sau để bật tất cả các dịch vụ GCP chúng ta sẽ sử dụng. Thay `[YOUR_PROJECT_ID]` bằng ID project của bạn.
    ```bash
    gcloud services enable \
        cloudfunctions.googleapis.com \
        cloudbuild.googleapis.com \
        cloudscheduler.googleapis.com \
        pubsub.googleapis.com \
        bigquery.googleapis.com \
        secretmanager.googleapis.com \
        eventarc.googleapis.com \
        --project=[YOUR_PROJECT_ID]
    ```

## 1. Cấu hình Biến môi trường và Secret

Để bảo mật API key và dễ dàng cấu hình, chúng ta sẽ sử dụng Secret Manager và các biến môi trường.

### Bước 1.1: Lưu API Key vào Secret Manager

Lưu API key của OpenWeatherMap vào Secret Manager để Cloud Functions có thể truy cập một cách an toàn.

```bash
# Tạo một secret mới
gcloud secrets create openweather-api-key --replication-policy="automatic" --project=[YOUR_PROJECT_ID]

# Thêm phiên bản đầu tiên cho secret với giá trị là API key của bạn
# Thay [YOUR_API_KEY] bằng key bạn đã lấy từ OpenWeatherMap
printf "[YOUR_API_KEY]" | gcloud secrets versions add openweather-api-key --data-file=-\- --project=[YOUR_PROJECT_ID]
```

### Bước 1.2: Chuẩn bị các biến môi trường

Hãy đặt các biến môi trường sau trong terminal của bạn để tiện sử dụng trong các lệnh tiếp theo. Bạn có thể tìm thấy Project ID và Project Number trên trang chủ Google Cloud Console.

```bash
export GCP_PROJECT_ID="[YOUR_PROJECT_ID]"
export GCP_PROJECT_NUMBER="[YOUR_PROJECT_NUMBER]" # ví dụ: 123456789012
export GCP_REGION="[YOUR_GCP_REGION]" # ví dụ: us-central1, asia-southeast1
```

*Lưu ý: Cloud Functions thế hệ thứ 2 (Gen 2) mặc định sử dụng Service Account của Compute Engine (`${GCP_PROJECT_NUMBER}-compute@developer.gserviceaccount.com`). Các bước gán quyền dưới đây sẽ cấp quyền cho Service Account này.*

## 2. Cấu hình Hạ tầng

### Bước 2.1: Tạo Dataset và Bảng trong BigQuery

Chúng ta cần một nơi để lưu trữ dữ liệu thô từ API.

```bash
# Tạo dataset
bq --location=US mk --dataset ${GCP_PROJECT_ID}:raw_data

# Tạo bảng với schema đã định nghĩa
bq mk --table \
    --description "Bảng lưu trữ dữ liệu JSON thô từ API thời tiết" \
    ${GCP_PROJECT_ID}:raw_data.weather_api_data \
    raw_json:JSON,inserted_at:TIMESTAMP
```
*Lưu ý: Lệnh trên sẽ tự động thêm trường `inserted_at` với giá trị mặc định là thời gian hiện tại khi có dòng mới được chèn.*

### Bước 2.2: Tạo Chủ đề (Topic) Pub/Sub

Đây là nơi function đầu tiên sẽ gửi tin nhắn đến.

```bash
gcloud pubsub topics create weather-stream --project=${GCP_PROJECT_ID}
```

## 3. Triển khai Cloud Functions

Chúng ta sẽ triển khai 2 functions nằm trong thư mục `weather_streaming`.

*Lưu ý: Để giải quyết lỗi triển khai, mã Python đã được hợp nhất vào một tệp `main.py` duy nhất. Các lệnh bên dưới đã được cập nhật để sử dụng runtime `python310`.*

### Bước 3.1: Gán quyền cho Service Account

Để các function có quyền truy cập các dịch vụ khác, chúng ta cần gán vai trò (roles) cho service account mà chúng sẽ sử dụng. 

```bash
# Định nghĩa biến SERVICE_ACCOUNT để dễ sử dụng, dựa trên Project Number đã thiết lập ở trên.
export SERVICE_ACCOUNT="${GCP_PROJECT_NUMBER}-compute@developer.gserviceaccount.com"

# Gán quyền truy cập secret
gcloud secrets add-iam-policy-binding openweather-api-key \
    --member="serviceAccount:${SERVICE_ACCOUNT}" \
    --role="roles/secretmanager.secretAccessor" \
    --project=${GCP_PROJECT_ID}

# Gán quyền ghi vào BigQuery
gcloud projects add-iam-policy-binding ${GCP_PROJECT_ID} \
    --member="serviceAccount:${SERVICE_ACCOUNT}" \
    --role="roles/bigquery.dataEditor"

# Gán quyền publish vào Pub/Sub
gcloud projects add-iam-policy-binding ${GCP_PROJECT_ID} \
    --member="serviceAccount:${SERVICE_ACCOUNT}" \
    --role="roles/pubsub.publisher"
```

### Bước 3.2: Triển khai Function 1: `fetch_weather_and_publish`

Function này lấy dữ liệu từ API và đẩy vào Pub/Sub.

```bash
gcloud functions deploy fetch_weather_and_publish \
    --gen2 \
    --runtime=python310 \
    --region=${GCP_REGION} \
    --source=./weather_streaming \
    --entry-point=fetch_weather_and_publish \
    --trigger-http \
    --allow-unauthenticated \
    --set-env-vars="GCP_PROJECT_ID=${GCP_PROJECT_ID},PUB_SUB_TOPIC_ID=weather-stream" \
    --set-secrets="OPENWEATHER_API_KEY=openweather-api-key:latest" \
    --project=${GCP_PROJECT_ID}
```
*Lưu ý: `--allow-unauthenticated` được dùng để Cloud Scheduler có thể gọi function này. Trong môi trường production, bạn nên cấu hình xác thực.*

### Bước 3.3: Triển khai Function 2: `insert_weather_data_to_bq`

Function này được kích hoạt bởi Pub/Sub và ghi dữ liệu vào BigQuery.

```bash
gcloud functions deploy insert_weather_data_to_bq \
    --gen2 \
    --runtime=python310 \
    --region=${GCP_REGION} \
    --source=./weather_streaming \
    --entry-point=insert_weather_data_to_bq \
    --trigger-topic=weather-stream \
    --set-env-vars="GCP_PROJECT_ID=${GCP_PROJECT_ID},BQ_DATASET_ID=raw_data,BQ_TABLE_ID=weather_api_data" \
    --project=${GCP_PROJECT_ID}
```

## 4. Lên lịch thực thi với Cloud Scheduler

Tạo một cron job để tự động gọi function `fetch_weather_and_publish` mỗi 15 phút.

```bash
# Lấy URL của function vừa deploy
FUNCTION_URL=$(gcloud functions describe fetch_weather_and_publish --gen2 --region=${GCP_REGION} --project=${GCP_PROJECT_ID} --format="value(serviceConfig.uri)")

# Tạo scheduler job
gcloud scheduler jobs create http weather-fetch-job \
    --schedule="*/15 * * * *" \
    --uri=${FUNCTION_URL} \
    --http-method=GET \
    --location=${GCP_REGION} \
    --description="Gọi API thời tiết mỗi 15 phút." \
    --project=${GCP_PROJECT_ID}
```

## 5. Chạy dbt

Sau khi pipeline streaming đã chạy và dữ liệu đã bắt đầu được ghi vào BigQuery, bạn có thể chạy dbt để cập nhật các model cuối cùng.

```bash
# Chạy toàn bộ các model trong project dbt của bạn
dbt run
```

Bây giờ, model `dim_weather` sẽ được xây dựng dựa trên dữ liệu thời gian thực thay vì dữ liệu tĩnh hàng ngày.

---
**Hoàn tất!** Bạn đã triển khai thành công một pipeline streaming hoàn chỉnh.
