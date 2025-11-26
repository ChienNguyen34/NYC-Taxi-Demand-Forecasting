import os
import json
import requests
import functions_framework
from google.cloud import pubsub_v1
import base64
from google.cloud import bigquery
import pytz
from datetime import datetime, timedelta
import random

# --- Cấu hình chung ---
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")

# --- Cấu hình cho Function 1: fetch_weather_and_publish ---
PUB_SUB_TOPIC_ID = os.environ.get("PUB_SUB_TOPIC_ID", "weather-stream")

# Correctly read the secret when deployed to a Gen 2 function
# The secret is mounted as a file at /secrets/SECRET_NAME
if os.path.exists('/secrets/OPENWEATHER_API_KEY'):
    with open('/secrets/OPENWEATHER_API_KEY', 'r', encoding='utf-8-sig') as f:
        OPENWEATHER_API_KEY = f.read().strip()
else:
    # Fallback for local development, where the secret is set as an env var
    OPENWEATHER_API_KEY = os.environ.get("OPENWEATHER_API_KEY", "").strip()

NYC_LAT = os.environ.get("NYC_LAT", "40.7128")
NYC_LON = os.environ.get("NYC_LON", "-74.0060")
OPENWEATHER_API_URL = "https://api.openweathermap.org/data/2.5/weather"

# Khởi tạo Publisher Client của Pub/Sub
publisher = pubsub_v1.PublisherClient()

# --- Cấu hình cho Function 2: insert_weather_data_to_bq ---
BQ_DATASET_ID = os.environ.get("BQ_DATASET_ID", "raw_data")
BQ_TABLE_ID = os.environ.get("BQ_TABLE_ID", "weather_api_data")

# --- Cấu hình cho Taxi Streaming ---
TAXI_TOPIC_ID = os.environ.get("TAXI_TOPIC_ID", "taxi-stream")
TAXI_DATASET_ID = os.environ.get("TAXI_DATASET_ID", "streaming")
TAXI_TABLE_ID = os.environ.get("TAXI_TABLE_ID", "processed_trips")
TRIPS_PER_BATCH = int(os.environ.get("TRIPS_PER_BATCH", "1000"))  # Số trips mỗi lần query

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


# ============================================================================
# TAXI STREAMING FUNCTIONS ADDED
# ============================================================================

@functions_framework.http
def fetch_taxi_trips_and_publish(request):
    """
    Cloud Function HTTP trigger.
    Query taxi trips từ BigQuery Public Dataset 2021, shift time sang 2025, publish vào Pub/Sub.
    """
    print("Function fetch_taxi_trips_and_publish started.")
    
    if not GCP_PROJECT_ID:
        error_msg = "Thiếu biến môi trường: GCP_PROJECT_ID là bắt buộc."
        print(f"ERROR: {error_msg}")
        return (error_msg, 500)
    
    topic_path = publisher.topic_path(GCP_PROJECT_ID, TAXI_TOPIC_ID)
    
    # Parse request parameters
    request_json = request.get_json(silent=True)
    request_args = request.args
    
    # Get date parameter (default to today - 4 years to simulate 2021 data)
    target_date_2025 = request_json.get('date') if request_json else request_args.get('date')
    if not target_date_2025:
        # Default: today's date in 2025, map back to 2021
        target_date_2025 = datetime.now().strftime('%Y-%m-%d')
    
    # Calculate 2021 date (subtract 4 years)
    date_2025 = datetime.strptime(target_date_2025, '%Y-%m-%d')
    date_2021 = date_2025 - timedelta(days=1461)  # 4 years = 1461 days
    date_2021_str = date_2021.strftime('%Y-%m-%d')
    
    print(f"Fetching trips for 2021 date: {date_2021_str} (will be shifted to {target_date_2025})")
    
    # Query public dataset - sample random trips with ALL fields
    query = f"""
    SELECT
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        passenger_count,
        trip_distance,
        pickup_location_id,
        dropoff_location_id,
        rate_code,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        imp_surcharge,
        airport_fee,
        total_amount
    FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2021`
    WHERE DATE(pickup_datetime) = '{date_2021_str}'
        AND trip_distance > 0
        AND passenger_count > 0
        AND total_amount > 0
        AND pickup_location_id IS NOT NULL
        AND dropoff_location_id IS NOT NULL
    ORDER BY RAND()
    LIMIT {TRIPS_PER_BATCH}
    """
    
    try:
        print(f"Executing query to fetch {TRIPS_PER_BATCH} trips...")
        query_job = bq_client.query(query)
        results = query_job.result()
        
        trips_published = 0
        for row in results:
            # Shift timestamps +4 years (1462 days for full 2025 year)
            pickup_2025 = row.pickup_datetime + timedelta(days=1462)
            dropoff_2025 = row.dropoff_datetime + timedelta(days=1462)
            
            # Create trip message with ALL fields
            trip_data = {
                "vendor_id": str(row.vendor_id),
                "pickup_datetime": pickup_2025.isoformat(),
                "dropoff_datetime": dropoff_2025.isoformat(),
                "passenger_count": int(row.passenger_count),
                "trip_distance": float(row.trip_distance),
                "pickup_location_id": str(row.pickup_location_id),
                "dropoff_location_id": str(row.dropoff_location_id),
                "rate_code": str(row.rate_code) if row.rate_code else "1",
                "payment_type": str(row.payment_type) if row.payment_type else "1",
                "fare_amount": float(row.fare_amount),
                "extra": float(row.extra) if row.extra else 0.0,
                "mta_tax": float(row.mta_tax) if row.mta_tax else 0.0,
                "tip_amount": float(row.tip_amount) if row.tip_amount else 0.0,
                "tolls_amount": float(row.tolls_amount) if row.tolls_amount else 0.0,
                "imp_surcharge": float(row.imp_surcharge) if row.imp_surcharge else 0.0,
                "airport_fee": float(row.airport_fee) if row.airport_fee else 0.0,
                "total_amount": float(row.total_amount)
            }
            
            # Publish to Pub/Sub
            message_data = json.dumps(trip_data).encode("utf-8")
            future = publisher.publish(topic_path, message_data)
            trips_published += 1
        
        success_msg = f"Published {trips_published} taxi trips to {topic_path} for date {target_date_2025}"
        print(success_msg)
        return (success_msg, 200)
        
    except Exception as e:
        error_msg = f"Lỗi khi query hoặc publish taxi trips: {e}"
        print(f"ERROR: {error_msg}")
        return (error_msg, 500)


@functions_framework.cloud_event
def insert_taxi_trips_to_bq(cloud_event):
    """
    Cloud Function Pub/Sub trigger.
    Đọc taxi trips từ Pub/Sub và insert vào BigQuery streaming table.
    """
    print("Function insert_taxi_trips_to_bq triggered.")
    
    if not GCP_PROJECT_ID:
        error_msg = "Thiếu biến môi trường: GCP_PROJECT_ID là bắt buộc."
        print(f"ERROR: {error_msg}")
        raise ValueError(error_msg)
    
    try:
        # Decode message from Pub/Sub
        message_data_base64 = cloud_event.data["message"]["data"]
        json_string = base64.b64decode(message_data_base64).decode("utf-8")
        trip_data = json.loads(json_string)
        print(f"Received trip data: pickup at {trip_data['pickup_datetime']}")
        
    except (KeyError, TypeError, base64.binascii.Error, json.JSONDecodeError) as e:
        error_msg = f"Lỗi khi đọc hoặc giải mã tin nhắn Pub/Sub: {e}"
        print(f"ERROR: {error_msg}")
        return
    
    try:
        # Insert to BigQuery streaming table
        table_id = f"{GCP_PROJECT_ID}.{TAXI_DATASET_ID}.{TAXI_TABLE_ID}"
        
        # Add processing timestamp
        current_time_utc = datetime.now(pytz.utc)
        
        row_to_insert = {
            "vendor_id": trip_data["vendor_id"],
            "pickup_datetime": trip_data["pickup_datetime"],
            "dropoff_datetime": trip_data["dropoff_datetime"],
            "passenger_count": trip_data["passenger_count"],
            "trip_distance": trip_data["trip_distance"],
            "pickup_location_id": trip_data["pickup_location_id"],
            "dropoff_location_id": trip_data["dropoff_location_id"],
            "rate_code": trip_data.get("rate_code", "1"),
            "payment_type": trip_data.get("payment_type", "1"),
            "fare_amount": trip_data["fare_amount"],
            "extra": trip_data.get("extra", 0.0),
            "mta_tax": trip_data.get("mta_tax", 0.0),
            "tip_amount": trip_data.get("tip_amount", 0.0),
            "tolls_amount": trip_data.get("tolls_amount", 0.0),
            "imp_surcharge": trip_data.get("imp_surcharge", 0.0),
            "airport_fee": trip_data.get("airport_fee", 0.0),
            "total_amount": trip_data["total_amount"],
            "processing_timestamp": current_time_utc.isoformat()
        }
        
        errors = bq_client.insert_rows_json(table_id, [row_to_insert])
        
        if not errors:
            print(f"Successfully inserted trip to {table_id}")
        else:
            error_msg = f"Errors when inserting to BigQuery: {errors}"
            print(f"ERROR: {error_msg}")
            
    except Exception as e:
        error_msg = f"Lỗi không xác định khi ghi taxi trip vào BigQuery: {e}"
        print(f"ERROR: {error_msg}")
