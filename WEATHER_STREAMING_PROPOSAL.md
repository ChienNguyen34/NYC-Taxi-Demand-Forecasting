# Kế hoạch triển khai luồng Streaming dữ liệu thời tiết

Tài liệu này mô tả kế hoạch chi tiết để xây dựng một pipeline streaming, lấy dữ liệu thời tiết từ Public API và đưa vào Data Warehouse (BigQuery) mỗi 15 phút.

## 1. Mục tiêu

- **Nguồn dữ liệu:** Lấy dữ liệu thời tiết gần thời gian thực cho thành phố New York từ một Public API.
- **Tần suất:** Cập nhật dữ liệu mới mỗi 15 phút (có thể cấu hình).
- **Đích:** Bảng `dim_weather` trong BigQuery.
- **Yêu cầu:** Dữ liệu mới phải tương thích hoàn toàn với schema hiện có của bảng `dim_weather` sau khi được xử lý. Luồng pipeline phải ổn định và có khả năng tự động hóa.

## 2. Phân tích & Thách thức

Bảng `dim_weather` hiện tại là một bảng **tổng hợp theo ngày** (daily aggregation), với mỗi dòng đại diện cho một ngày duy nhất. Việc chèn dữ liệu "mỗi 15 phút" trực tiếp vào bảng này sẽ phá vỡ cấu trúc dữ liệu.

**Giải pháp:** Chúng ta sẽ không ghi trực tiếp vào `dim_weather`. Thay vào đó, chúng ta sẽ xây dựng một pipeline streaming để ghi dữ liệu "thô" vào một bảng trung gian trong BigQuery. Sau đó, mô hình dbt hiện tại sẽ được điều chỉnh để đọc từ bảng trung gian này và tổng hợp dữ liệu theo ngày để cập nhật vào `dim_weather`.

## 3. Kiến trúc được đề xuất

```text
.--------------------.
|  Cloud Scheduler   |  (Kích hoạt mỗi 15 phút)
| (Cron Job)         |
'--------------------'
         |
         v
.--------------------.
|   Cloud Function   |  (1. Lấy dữ liệu từ API thời tiết)
|  (API Fetcher)     |
'--------------------'
         |
         v
.--------------------.
|   Pub/Sub Topic    |  (2. Gửi tin nhắn chứa JSON thô)
|  'weather-stream'  |
'--------------------'
         |
         v (Kích hoạt khi có tin nhắn mới)
.--------------------.
|   Cloud Function   |  (3. Đọc tin nhắn từ Pub/Sub)
|   (BQ Inserter)    |
'--------------------'
         |
         v (Ghi dữ liệu vào)
.---------------------.
| BigQuery Table      |  (4. Lưu trữ JSON thô)
|'raw_weather_api_data'|
'---------------------'
         |
         | (Được sử dụng bởi dbt trong lần chạy hàng ngày)
         v
.---------------------.
|   dbt Staging Model |
|'stg_streaming_weather'|
'---------------------'
```

**Luồng xử lý:**

1.  **Cloud Scheduler:** Kích hoạt một Cloud Function mỗi 15 phút.
2.  **Cloud Function (API Fetcher):** Một Python function sẽ:
    - Gọi đến **OpenWeatherMap API** để lấy dữ liệu thời tiết hiện tại cho NYC.
    - Gửi dữ liệu JSON thô vào một **Pub/Sub Topic** có tên `weather-stream`.
3.  **Cloud Function (BQ Inserter):** Một Python function thứ hai sẽ:
    - Được kích hoạt mỗi khi có tin nhắn mới trên topic `weather-stream`.
    - Chèn trực tiếp dữ liệu JSON vào một bảng BigQuery có tên `raw_weather_api_data`. Bảng này sẽ lưu trữ lịch sử dữ liệu thời tiết theo từng phút.
4.  **dbt Transformation:**
    - Airflow DAG hiện tại sẽ tiếp tục chạy hàng ngày.
    - Một model dbt staging mới (`stg_streaming_weather.sql`) sẽ đọc dữ liệu từ bảng `raw_weather_api_data`, làm sạch và chuyển đổi nó.
    - Model `dim_weather.sql` sẽ được cập nhật để sử dụng `stg_streaming_weather.sql` làm nguồn, tổng hợp dữ liệu 15 phút thành dữ liệu hàng ngày.

## 4. Chi tiết triển khai

### Bước 1: Thiết lập hạ tầng (Thực hiện thủ công trên GCP)

1.  **BigQuery:**
    - Tạo một dataset mới có tên `raw_data` (nếu chưa có).
    - Tạo bảng `raw_weather_api_data` với schema linh hoạt để chứa dữ liệu JSON.
      ```sql
      CREATE TABLE nyc-taxi-project-477115.raw_data.weather_api_data (
          raw_json JSON,
          inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
      );
      ```
2.  **Pub/Sub:** Tạo một topic mới có tên `weather-stream`.
3.  **API Key:** Đăng ký tài khoản miễn phí trên [OpenWeatherMap](https://openweathermap.org/api) và lấy API Key. Key này sẽ được lưu dưới dạng Secret trong Google Secret Manager.

### Bước 2: Tạo mã nguồn cho Cloud Functions

Pipeline này sử dụng hai Cloud Functions để thực hiện các tác vụ một cách tự động.

#### Google Cloud Functions là gì?

Google Cloud Functions là một dịch vụ tính toán **serverless** (không cần máy chủ) và **event-driven** (kích hoạt theo sự kiện).
-   **Serverless:** Bạn chỉ cần tải mã nguồn lên, Google sẽ lo toàn bộ phần hạ tầng, mở rộng và bảo trì.
-   **Event-driven:** Mỗi function sẽ chạy để phản hồi một sự kiện cụ thể.

Trong pipeline này:
1.  **`fetch_weather.py` (Function 1 - API Fetcher):** Được kích hoạt bởi **Cloud Scheduler** mỗi 15 phút. Nhiệm vụ của nó là gọi đến API thời tiết bên ngoài và gửi dữ liệu thô vào Pub/Sub.
2.  **`insert_to_bq.py` (Function 2 - BQ Inserter):** Được kích hoạt bởi **Pub/Sub** mỗi khi có tin nhắn mới. Nhiệm vụ của nó là đọc tin nhắn đó và ghi vào bảng BigQuery.

#### Chi phí và Gói miễn phí (Free Tier)

Dịch vụ Google Cloud Functions có một gói miễn phí hàng tháng rất hào phóng, đủ để chạy pipeline này mà không tốn chi phí trong giai đoạn phát triển và quy mô nhỏ.

-   **Gói miễn phí hàng tháng bao gồm:**
    -   **2 triệu lượt gọi**
    -   **400.000 GB-giây** thời gian tính toán bộ nhớ
    -   **200.000 GHz-giây** thời gian tính toán CPU
    -   **5 GB** dữ liệu truyền ra ngoài Internet

Với tần suất 15 phút/lần, function `API Fetcher` sẽ chỉ chạy `4 * 24 * 30 = 2,880` lần mỗi tháng, thấp hơn rất nhiều so với giới hạn 2 triệu lượt gọi của gói miễn phí. Do đó, chi phí cho Cloud Functions trong dự án này gần như bằng 0.

#### Mã nguồn cần tạo

Sẽ có 2 file Python mới được tạo trong một thư mục mới, ví dụ `weather_streaming/`.

1.  **`fetch_weather.py`:** Chứa logic để gọi API OpenWeatherMap và publish tin nhắn vào Pub/Sub.
2.  **`insert_to_bq.py`:** Chứa logic để đọc tin nhắn từ Pub/Sub và ghi vào bảng `raw_weather_api_data`.

### Bước 3: Cập nhật dbt Project

1.  **`sources.yml`:** Khai báo bảng `raw_weather_api_data` như một source mới.
    ```yaml
    # models/sources.yml
    ...
      - name: raw_data
        tables:
          - name: weather_api_data
    ```
2.  **Tạo `stg_streaming_weather.sql`:**
    - Model này sẽ đọc từ `source('raw_data', 'weather_api_data')`.
    - Trích xuất và làm sạch các trường từ cột JSON (nhiệt độ, lượng mưa, cờ mưa/tuyết).
    - Chuyển đổi đơn vị (ví dụ: Kelvin sang Fahrenheit) để khớp với logic của `stg_weather.sql` cũ.
    - Kết quả là một bảng có cấu trúc tương tự như `stg_weather.sql` cũ nhưng với dữ liệu chi tiết hơn.
3.  **Cập nhật `dim_weather.sql`:**
    - Thay đổi `ref('stg_weather')` thành `ref('stg_streaming_weather')`.
    - Logic `GROUP BY observation_date` sẽ vẫn giữ nguyên, đảm bảo cấu trúc cuối cùng của `dim_weather` không thay đổi.

## 5. Kế hoạch tiếp theo

1.  **Review:** Chờ bạn xác nhận kế hoạch này.
2.  **Triển khai:**
    - Tạo các file Python cho Cloud Functions.
    - Cập nhật các file dbt như đã mô tả.
    - Cung cấp hướng dẫn để bạn có thể tự cấu hình hạ tầng trên GCP.

---
Vui lòng xem xét và cho tôi biết nếu bạn muốn tiếp tục theo kế hoạch này.
