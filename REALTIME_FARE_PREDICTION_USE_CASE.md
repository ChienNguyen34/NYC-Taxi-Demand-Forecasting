# Use Case: Dự đoán giá cước Taxi thời gian thực qua API (với Mapbox)

Tài liệu này định nghĩa luồng xử lý cho một API có khả năng dự đoán giá cước taxi ngay lập tức, sử dụng **Mapbox** để ước tính lộ trình và dữ liệu có sẵn trong Data Warehouse.

## 1. Mục tiêu

Xây dựng một API endpoint (`/predict_fare`) nhận thông tin về một chuyến đi (điểm đi, điểm đến) và trả về giá cước ước tính gần như ngay lập tức. Hệ thống phải sử dụng các thông tin mới nhất có thể, bao gồm dữ liệu lộ trình từ Mapbox.

## 2. Kiến trúc tổng quan

Kiến trúc này tập trung vào một mô hình **Request-Response** đồng bộ và tích hợp gọi API của Mapbox để làm giàu dữ liệu.

```text
                 ┌────────────────────────┐
                 │      User's App        │
                 └──────────┬─────────────┘
                            │ 1. POST /predict_fare
                            │    {pickup, dropoff}
                            ▼
                 ┌────────────────────────┐
                 │   API Endpoint         │───┐ 2. Call External API
                 │  (Google Cloud Function) │   │ (Mapbox Directions)
                 └──────────┬─────────────┘   │
                            │ 3. Receive distance & duration
                            ▼                  │
                 ┌────────────────────────┐   │
                 │   API Endpoint         │<──┘
                 └──────────┬─────────────┘
                            │ 4. Build & Run SQL Query
                            │    (Feature Lookups + ML.PREDICT)
                            ▼
                 ┌────────────────────────┐
                 │   BigQuery Data Warehouse  │
                 │  (BQML Model + Data Tables)│
                 └──────────┬─────────────┘
                            │ 5. Return predicted_fare
                            ▼
                 ┌────────────────────────┐
                 │   API Endpoint         │
                 └──────────┬─────────────┘
                            │ 6. Return JSON Response
                            │    { "predicted_fare": 18.5 }
                            ▼
                 ┌────────────────────────┐
                 │      User's App        │
                 └────────────────────────┘
```

## 3. Định nghĩa API

**Endpoint:** `POST /predict_fare`

**Request Body (JSON):**
```json
{
  "pickup_latitude": 40.7580,
  "pickup_longitude": -73.9855,
  "dropoff_latitude": 40.7831,
  "dropoff_longitude": -73.9712,
  "passenger_count": 1
}
```

**Success Response (JSON):**
```json
{
  "predicted_fare": 18.50,
  "currency": "USD"
}
```

## 4. Phân tích: Sử dụng Mapbox API để ước tính Lộ trình

Để có được `trip_distance` và `trip_duration` cho một chuyến đi chưa diễn ra, chúng ta sẽ sử dụng **Mapbox Directions API**.

- **Tài liệu chính thức:** [Mapbox Directions API Documentation](https://docs.mapbox.com/api/navigation/directions/)

### Luồng hoạt động chi tiết

1.  **Chuẩn bị lời gọi API:**
    -   API Endpoint của chúng ta (Cloud Function) sẽ nhận tọa độ điểm đi (`pickup`) và điểm đến (`dropoff`).
    -   Nó sẽ xây dựng một URL để thực hiện lời gọi `GET` đến Mapbox.

2.  **Cấu trúc lời gọi API (Example API Call):**
    -   URL có định dạng: `https://api.mapbox.com/directions/v5/mapbox/{profile}/{coordinates}`
    -   `{profile}`: Phương tiện di chuyển, thường là `driving-traffic` để có kết quả tốt nhất cho xe hơi.
    -   `{coordinates}`: Chuỗi tọa độ theo định dạng `longitude,latitude` được ngăn cách bởi dấu chấm phẩy (`;`).
    -   Cần có `access_token` của bạn làm tham số truy vấn.

    **Ví dụ cụ thể:**
    ```http
    GET https://api.mapbox.com/directions/v5/mapbox/driving-traffic/-73.9855,40.7580;-73.9712,40.7831?access_token=YOUR_MAPBOX_ACCESS_TOKEN
    ```

3.  **Xử lý dữ liệu trả về (API Response):**
    -   Mapbox sẽ trả về một đối tượng JSON. Dữ liệu chúng ta cần nằm trong đối tượng `routes` đầu tiên.

    **Ví dụ JSON Response (đã rút gọn):**
    ```json
    {
      "routes": [
        {
          "distance": 3648.8,
          "duration": 658.9,
          "legs": [
            {
              "distance": 3648.8,
              "duration": 658.9,
              "summary": "Broadway, Central Park West",
              "steps": []
            }
          ],
          "weight_name": "routability",
          "weight": 735.2,
          "geometry": "..."
        }
      ],
      "waypoints": [
        {
          "distance": 7.5,
          "name": "West 46th Street",
          "location": [-73.985506, 40.758013]
        },
        {
          "distance": 10.1,
          "name": "West 82nd Street",
          "location": [-73.97124, 40.78311]
        }
      ],
      "code": "Ok",
      "uuid": "ab1234cd-..."
    }
    ```

4.  **Trích xuất dữ liệu:**
    -   Code trong Cloud Function sẽ phân tích (parse) chuỗi JSON này.
    -   Lấy giá trị từ `routes[0].distance` (khoảng cách, tính bằng mét).
    -   Lấy giá trị từ `routes[0].duration` (thời gian di chuyển, tính bằng giây).
    -   Hai giá trị này sau đó sẽ được dùng làm feature đầu vào cho mô hình BQML.

### Phân tích Ưu/Nhược điểm (so với Google Maps)

| Tiêu chí | Ưu điểm (Pros) | Nhược điểm (Cons) |
| :--- | :--- | :--- |
| **Chi phí** | **Gói miễn phí thường lớn hơn** cho nhiều dịch vụ, có thể tiết kiệm chi phí hơn ở quy mô nhỏ và vừa. | Chi phí khi vượt gói miễn phí có thể vẫn đáng kể. |
| **Tùy biến** | **Rất mạnh về tùy biến giao diện bản đồ** (dù không trực tiếp áp dụng cho backend API này, nhưng là một điểm mạnh của hệ sinh thái Mapbox). | - |
| **Độ chính xác** | Dữ liệu tốt, nhưng ở một số khu vực có thể không chi tiết bằng Google. | - |
| **Độ trễ & Tin cậy** | - | Tương tự các API ngoài khác: **Tăng độ trễ** cho API và **phụ thuộc vào dịch vụ của bên thứ ba**. |

**Kết luận:** Mapbox là một sự thay thế tuyệt vời cho Google Maps, đặc biệt hấp dẫn nhờ gói miễn phí lớn và khả năng tùy biến. Đối với việc lấy dữ liệu lộ trình ở backend, nó hoạt động hiệu quả tương tự Google Maps.

## 5. Feature Engineering & Lookup trong thời gian thực

Khi nhận được yêu cầu, API sẽ thực hiện các bước sau:
1.  **Gọi Mapbox Directions API** để lấy `trip_distance` và `trip_duration`.
2.  **Xây dựng câu lệnh SQL** để tra cứu các feature còn lại từ BigQuery.

| Feature | Nguồn gốc & Cách lấy trong thời gian thực | Bảng dữ liệu phụ thuộc |
| :--- | :--- | :--- |
| `trip_distance` <br> `trip_duration_seconds` | **Lấy từ phản hồi của Mapbox Directions API.** | - (API ngoài) |
| `pickup_h3_id` <br> `dropoff_h3_id` | Tính toán từ `latitude` và `longitude` trong request body bằng các hàm H3. | - |
| `hour_of_day` <br> `day_of_week` | Trích xuất từ timestamp hiện tại của server API (`CURRENT_TIMESTAMP`). | - |
| `is_holiday` | Tra cứu (lookup) vào bảng `dim_datetime` dựa trên ngày hiện tại (`CURRENT_DATE`). | `marts.dimensions.dim_datetime` |
| `latest_temperature` <br> `latest_precipitation` | Lấy bản ghi thời tiết **gần nhất** từ bảng chứa dữ liệu weather streaming. <br> `SELECT temp, prcp FROM raw_weather_stream ORDER BY timestamp DESC LIMIT 1` | `raw_data.weather_api_data` |
| `predicted_demand` | Tra cứu nhu cầu dự báo từ bảng kết quả của mô hình demand forecast, dựa trên `pickup_h3_id` và `hour_of_day` hiện tại. | `ml_predictions.hourly_demand_forecast` |


## 6. Logic truy vấn SQL tổng hợp

Sau khi đã có `trip_distance` và `trip_duration` từ Mapbox, API sẽ truyền chúng như tham số vào câu lệnh SQL.

```sql
-- Pseudo-SQL cho API Endpoint
-- Tham số đầu vào: @trip_distance_meters, @trip_duration_seconds, @pickup_lat, @pickup_lon

WITH
  -- 1. Lấy thông tin thời tiết mới nhất
  latest_weather AS (
    SELECT
      json_value(raw_json, '$.main.temp') as temperature,
      json_value(raw_json, '$.rain."1h"') as precipitation
    FROM `raw_data.weather_api_data`
    ORDER BY inserted_at DESC
    LIMIT 1
  ),
  -- 2. Lấy thông tin ngày lễ
  holiday_info AS (
    SELECT is_holiday
    FROM `marts.dimensions.dim_datetime`
    WHERE date = CURRENT_DATE('Asia/Ho_Chi_Minh')
  ),
  -- 3. Lấy nhu cầu dự báo
  demand_info AS (
    SELECT predicted_demand
    FROM `ml_predictions.hourly_demand_forecast`
    WHERE pickup_h3_id = H3_FROM_COORDS(@pickup_lat, @pickup_lon, 10)
      AND timestamp_hour = TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), HOUR)
  )
-- 4. Gọi mô hình dự đoán với tất cả các feature đã thu thập
SELECT
  predicted_fare
FROM
  ML.PREDICT(
    MODEL `ml_models.fare_estimation_model`,
    (
      SELECT
        -- Features từ API ngoài (Mapbox)
        @trip_distance_meters AS trip_distance,
        @trip_duration_seconds AS trip_duration_seconds,

        -- Features tra cứu từ BigQuery
        (SELECT temperature FROM latest_weather) AS avg_temperature,
        (SELECT precipitation FROM latest_weather) AS precipitation_amount,
        (SELECT is_holiday FROM holiday_info) AS is_holiday,
        (SELECT predicted_demand FROM demand_info) AS predicted_demand,
        
        -- Features tính toán tức thì
        EXTRACT(HOUR FROM CURRENT_TIMESTAMP()) AS hour_of_day,
        H3_FROM_COORDS(@pickup_lat, @pickup_lon, 10) AS pickup_h3_cell_id
    )
  );
```

## 7. Các thành phần phụ thuộc (Prerequisites)

Để API này hoạt động, các thành phần sau phải được xây dựng và cập nhật thường xuyên:
1.  **BQML Model:** `ml_models.fare_estimation_model` phải được huấn luyện.
2.  **Mapbox Access Token:** Phải có một access token hợp lệ từ tài khoản Mapbox của bạn và được lưu trữ an toàn (ví dụ: Google Secret Manager).
3.  **Weather Data:** Pipeline streaming weather phải đang chạy và cập nhật bảng `raw_data.weather_api_data`.
4.  **Demand Forecast:** Mô hình dự báo nhu cầu phải chạy và cập nhật bảng `ml_predictions.hourly_demand_forecast`.

## 8. Pipeline Huấn luyện Mô hình (Model Training Pipeline)

Để mô hình `ml_models.fare_estimation_model` có thể hoạt động, nó cần được huấn luyện (train) và tái huấn luyện (retrain) định kỳ trên dữ liệu lịch sử. Luồng pipeline này được tích hợp vào dbt và Airflow.

### 8.1. Chuẩn bị dữ liệu training với dbt

Một model dbt mới sẽ được tạo để xây dựng bộ dữ liệu training.

-   **Tên model:** `fct_fare_prediction_training`
-   **Nguồn dữ liệu:**
    -   `fct_trips`: Chứa thông tin chi tiết về các chuyến đi, bao gồm **`fare_amount`** (biến mục tiêu), `trip_distance`, `passenger_count`, và thời gian di chuyển.
    -   `dim_location`: Để chuyển đổi `location_id` thành `h3_cell_id`.
    -   `dim_datetime`: Để lấy các feature về thời gian như `hour_of_day`, `day_of_week`, `is_holiday`.
    -   `dim_weather`: Để lấy thông tin thời tiết lịch sử (`avg_temp_celsius`, `total_precipitation_mm`).
    -   `agg_hourly_demand_h3`: Để lấy thông tin về nhu cầu thực tế tại một khu vực và thời điểm (`historical_demand`), dùng làm proxy cho feature `predicted_demand` trong lúc training.
-   **Output:** Một bảng chứa đầy đủ các feature và biến mục tiêu, sẵn sàng cho việc huấn luyện. Các feature này tương đồng với các feature được sử dụng trong luồng dự đoán thời gian thực.

### 8.2. Huấn luyện mô hình với BQML

Một script SQL sẽ được tạo để thực hiện việc huấn luyện bằng BigQuery ML.

-   **Tên script:** `bqml_scripts/train_fare_model.sql`
-   **Logic:** Sử dụng câu lệnh `CREATE OR REPLACE MODEL` để huấn luyện một mô hình hồi quy (ví dụ: `BOOSTED_TREE_REGRESSOR`).
    -   **`MODEL`**: `ml_models.fare_estimation_model`
    -   **`INPUT(label)`**: `fare_amount`
    -   **`DATA`**: `SELECT * FROM {{ ref('fct_fare_prediction_training') }}`

### 8.3. Tích hợp vào Airflow

Luồng huấn luyện sẽ được thêm vào DAG `nyc_taxi_dag.py` như một task mới.

-   **Tên task:** `train_fare_model`
-   **Loại task:** `BigQueryOperator` (hoặc `BashOperator` gọi `bq query`).
-   **Thứ tự chạy:** Task này sẽ được thực thi **sau khi** tất cả các dbt model mà nó phụ thuộc (như `fct_fare_prediction_training`) đã được build thành công.

Kiến trúc này đảm bảo rằng mô hình dự đoán giá cước luôn được cập nhật với dữ liệu mới nhất, giúp duy trì độ chính xác của API.

## 9. Demo UI cho Use Case này (Sử dụng Streamlit)

Để minh họa và tương tác trực quan với use case dự đoán giá cước thời gian thực, chúng ta sẽ xây dựng một giao diện người dùng đơn giản nhưng hiệu quả bằng Streamlit.

### Mục tiêu của Demo UI

-   Cung cấp một cách trực quan để người dùng tương tác với mô hình dự đoán giá.
-   Trực quan hóa các yếu tố đầu vào (điểm đi/đến, vùng nhu cầu cao, thời tiết).
-   Mô phỏng kết quả dự đoán giá cước.

### Công nghệ sử dụng

-   **Streamlit:** Framework Python để xây dựng ứng dụng web dữ liệu nhanh chóng.
-   **Streamlit-Folium:** Thư viện để tích hợp bản đồ Folium tương tác vào Streamlit.

### Các tính năng chính của Demo UI

1.  **Bản đồ tương tác:**
    -   Hiển thị bản đồ khu vực NYC.
    -   Người dùng có thể **nhấp chuột trực tiếp lên bản đồ** để chọn điểm đón (Pickup) và điểm đến (Drop-off).
    -   Các điểm đã chọn sẽ được đánh dấu bằng icon rõ ràng trên bản đồ.
    -   Một đường kẻ sẽ nối điểm đón và điểm đến.
2.  **Trực quan hóa Vùng nhu cầu cao (High-Demand Zones):**
    -   Các khu vực được giả định có nhu cầu taxi cao sẽ được hiển thị dưới dạng các vùng tô màu (ví dụ: màu đỏ bán trong suốt) trực tiếp trên bản đồ.
3.  **Hiển thị thông tin thời tiết:**
    -   Mô phỏng hiển thị thông tin thời tiết hiện tại (nhiệt độ, điều kiện, độ ẩm, tốc độ gió) để minh họa cách dữ liệu thời tiết được sử dụng.
4.  **Mô phỏng Dự đoán giá cước:**
    -   Một nút "Predict Fare" sẽ kích hoạt quá trình mô phỏng gọi API.
    -   Sau một khoảng thời gian chờ ngắn (mô phỏng độ trễ API), một giá cước dự đoán (giá trị cố định hoặc ngẫu nhiên) sẽ được hiển thị rõ ràng.
5.  **Giao diện thân thiện:**
    -   Sử dụng layout hai cột để tối ưu không gian hiển thị (bản đồ một bên, điều khiển và kết quả một bên).
    -   Sử dụng các icon và định dạng văn bản để làm cho giao diện đẹp mắt và dễ hiểu.

### Lưu ý quan trọng

-   Trong bản demo này, việc gọi API dự đoán giá và lấy dữ liệu thời tiết/demand là **hoàn toàn giả lập** (fixed value hoặc random). Mục đích chính là để trình bày giao diện người dùng và luồng tương tác.
-   Để chạy bản demo này, bạn cần cài đặt các thư viện trong `dashboard_requirements.txt` và chạy file `streamlit_dashboard.py`.

### Cách chạy Demo

1.  Đảm bảo bạn đã cài đặt các thư viện cần thiết:
    ```bash
    pip install -r dashboard_requirements.txt
    ```
2.  Chạy ứng dụng Streamlit từ terminal:
    ```bash
    streamlit run streamlit_dashboard.py
    ```
