import os
import json
import requests
import functions_framework
from google.cloud import pubsub_v1
import base64
from google.cloud import bigquery
import pytz
from datetime import datetime

# --- Cấu hình chung ---
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")

# --- Cấu hình cho Function 1: fetch_weather_and_publish ---
PUB_SUB_TOPIC_ID = os.environ.get("PUB_SUB_TOPIC_ID", "weather-stream")

# Correctly read the secret when deployed to a Gen 2 function
# The secret is mounted as a file at /secrets/SECRET_NAME
if os.path.exists('/secrets/OPENWEATHER_API_KEY'):
    with open('/secrets/OPENWEATHER_API_KEY', 'r') as f:
        OPENWEATHER_API_KEY = f.read().strip()
else:
    # Fallback for local development, where the secret is set as an env var
    OPENWEATHER_API_KEY = os.environ.get("OPENWEATHER_API_KEY")

NYC_LAT = os.environ.get("NYC_LAT", "40.7128")
NYC_LON = os.environ.get("NYC_LON", "-74.0060")
OPENWEATHER_API_URL = "https://api.openweathermap.org/data/2.5/weather"

# Khởi tạo Publisher Client của Pub/Sub
publisher = pubsub_v1.PublisherClient()

# --- Cấu hình cho Function 2: insert_weather_data_to_bq ---
BQ_DATASET_ID = os.environ.get("BQ_DATASET_ID", "raw_data")
BQ_TABLE_ID = os.environ.get("BQ_TABLE_ID", "weather_api_data")

# Khởi tạo BigQuery Client
bq_client = bigquery.Client()


@functions_framework.http
def fetch_weather_and_publish(request):
    """
    Cloud Function được kích hoạt bởi HTTP.
    Lấy dữ liệu thời tiết từ OpenWeatherMap API và publish vào Pub/Sub.
    """
    print("Function fetch_weather_and_publish started.")
    if not all([GCP_PROJECT_ID, OPENWEATHER_API_KEY]):
        error_msg = "Thiếu biến môi trường: GCP_PROJECT_ID và OPENWEATHER_API_KEY là bắt buộc."
        print(f"ERROR: {error_msg}")
        return (error_msg, 500)

    topic_path = publisher.topic_path(GCP_PROJECT_ID, PUB_SUB_TOPIC_ID)

    try:
        params = {
            "lat": NYC_LAT,
            "lon": NYC_LON,
            "appid": OPENWEATHER_API_KEY,
            "units": "metric",
        }
        response = requests.get(OPENWEATHER_API_URL, params=params)
        response.raise_for_status()
        weather_data = response.json()
        print(f"Lấy dữ liệu thời tiết thành công: {weather_data}")

    except requests.exceptions.RequestException as e:
        error_msg = f"Lỗi khi gọi OpenWeatherMap API: {e}"
        print(f"ERROR: {error_msg}")
        return (error_msg, 500)

    try:
        message_data = json.dumps(weather_data).encode("utf-8")
        future = publisher.publish(topic_path, message_data)
        message_id = future.result()  # Chờ cho đến khi publish hoàn tất
        success_msg = f"Tin nhắn {message_id} đã được publish thành công lên topic {topic_path}."
        print(success_msg)
        return (success_msg, 200)

    except Exception as e:
        error_msg = f"Lỗi khi publish tin nhắn vào Pub/Sub: {e}"
        print(f"ERROR: {error_msg}")
        return (error_msg, 500)


@functions_framework.cloud_event
def insert_weather_data_to_bq(cloud_event):
    """
    Cloud Function được kích hoạt bởi một tin nhắn Pub/Sub (CloudEvent).
    Đọc dữ liệu từ tin nhắn và ghi vào bảng BigQuery.
    """
    if not GCP_PROJECT_ID:
        error_msg = "Thiếu biến môi trường: GCP_PROJECT_ID là bắt buộc."
        print(f"ERROR: {error_msg}")
        raise ValueError(error_msg)

    try:
        message_data_base64 = cloud_event.data["message"]["data"]
        json_string = base64.b64decode(message_data_base64).decode("utf-8")
        data_dict = json.loads(json_string)
        print(f"Đã nhận và giải mã dữ liệu: {data_dict}")

    except (KeyError, TypeError, base64.binascii.Error, json.JSONDecodeError) as e:
        error_msg = f"Lỗi khi đọc hoặc giải mã tin nhắn Pub/Sub: {e}"
        print(f"ERROR: {error_msg}")
        return

    try:
        table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}"
        current_time_utc_aware = datetime.now(pytz.utc)
        bigquery_timestamp_string = current_time_utc_aware.isoformat().replace('+00:00', 'Z')
        rows_to_insert = [{
            "raw_json": json_string,
            "inserted_at": bigquery_timestamp_string
        }]
        errors = bq_client.insert_rows_json(table_id, rows_to_insert)

        if not errors:
            print(f"Đã ghi thành công {len(rows_to_insert)} dòng vào bảng {table_id}")
        else:
            error_msg = f"Gặp lỗi khi ghi vào BigQuery: {errors}"
            print(f"ERROR: {error_msg}")

    except Exception as e:
        error_msg = f"Lỗi không xác định khi ghi vào BigQuery: {e}"
        print(f"ERROR: {error_msg}")
