# Setup NYC Taxi Project từ đầu

## 📋 Tổng quan
Guide này hướng dẫn setup lại toàn bộ project sau khi xóa BigQuery datasets.

---

## 🎯 BƯỚC 1: Tạo BigQuery Datasets & Tables

### 1.1. Tạo Datasets
```sql
-- Dataset cho raw streaming data
CREATE SCHEMA IF NOT EXISTS `nyc-taxi-project-477115.raw_data`
OPTIONS(
  location='US'
);

-- Dataset cho streaming trips
CREATE SCHEMA IF NOT EXISTS `nyc-taxi-project-477115.streaming`
OPTIONS(
  location='US'
);

-- Dataset cho staging (dbt tạo tự động nhưng có thể tạo trước)
CREATE SCHEMA IF NOT EXISTS `nyc-taxi-project-477115.staging`
OPTIONS(
  location='US'
);

-- Dataset cho dimensions (dbt tạo)
CREATE SCHEMA IF NOT EXISTS `nyc-taxi-project-477115.dimensions`
OPTIONS(
  location='US'
);

-- Dataset cho facts (dbt tạo)
CREATE SCHEMA IF NOT EXISTS `nyc-taxi-project-477115.facts`
OPTIONS(
  location='US'
);

-- Dataset cho ML models
CREATE SCHEMA IF NOT EXISTS `nyc-taxi-project-477115.ml_models`
OPTIONS(
  location='US'
);

-- Dataset cho ML predictions
CREATE SCHEMA IF NOT EXISTS `nyc-taxi-project-477115.ml_predictions`
OPTIONS(
  location='US'
);
```

### 1.2. Tạo Raw Weather Table (cho streaming)
```sql
CREATE TABLE `nyc-taxi-project-477115.raw_data.weather_api_data` (
    raw_json JSON,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(inserted_at)
OPTIONS(
  description='Raw weather data from OpenWeather API streaming'
);
```

### 1.3. Tạo Streaming Trips Table
```sql
CREATE TABLE `nyc-taxi-project-477115.streaming.processed_trips` (
    vendor_id STRING,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count INT64,
    trip_distance FLOAT64,
    pickup_location_id STRING,
    dropoff_location_id STRING,
    fare_amount FLOAT64,
    total_amount FLOAT64,
    processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(pickup_datetime)
OPTIONS(
  description='Streaming taxi trips data (2021 shifted to 2025)'
);
```

---

## 🎯 BƯỚC 2: Setup Pub/Sub (cho Cloud Functions)

### 2.1. Tạo Topics
```bash
# Weather topic
gcloud pubsub topics create weather-stream --project=nyc-taxi-project-477115

# Taxi topic  
gcloud pubsub topics create taxi-stream --project=nyc-taxi-project-477115
```

### 2.2. Tạo Subscriptions (cho Cloud Functions triggers)
```bash
# Weather subscription
gcloud pubsub subscriptions create weather-stream-sub \
  --topic=weather-stream \
  --project=nyc-taxi-project-477115

# Taxi subscription
gcloud pubsub subscriptions create taxi-stream-sub \
  --topic=taxi-stream \
  --project=nyc-taxi-project-477115
```

---

## 🎯 BƯỚC 3: Chạy dbt (Tạo Dimensions & Facts)

### 3.1. Install dbt dependencies
```bash
cd nyc_taxi_pipeline
dbt deps --profiles-dir .
```

### 3.2. Load seed data (events calendar)
```bash
dbt seed --profiles-dir .
```

### 3.3. Run staging models
```bash
# Chạy staging models (sẽ query public datasets + fake time)
dbt run --select staging --profiles-dir .
```

### 3.4. Run dimension models
```bash
# Tạo dim_datetime, dim_location, dim_weather
dbt run --select marts.dimensions --profiles-dir .
```

### 3.5. Run fact models (CẦN CÓ STREAMING DATA TRƯỚC)
```bash
# Chỉ chạy SAU KHI có data trong streaming.processed_trips
dbt run --select marts.facts --profiles-dir .
```

### 3.6. Test models
```bash
dbt test --profiles-dir .
```

---

## 🎯 BƯỚC 4: Deploy Cloud Functions (Optional - cho streaming)

### 4.1. Deploy Weather Functions
```bash
# Function 1: Fetch weather from API
gcloud functions deploy fetch-weather \
  --gen2 \
  --runtime python311 \
  --region us-central1 \
  --entry-point fetch_weather_and_publish \
  --trigger-http \
  --allow-unauthenticated \
  --source streaming \
  --set-env-vars GCP_PROJECT_ID=nyc-taxi-project-477115,PUB_SUB_TOPIC_ID=weather-stream \
  --set-secrets OPENWEATHER_API_KEY=OPENWEATHER_API_KEY:latest

# Function 2: Insert weather to BigQuery
gcloud functions deploy insert-weather \
  --gen2 \
  --runtime python311 \
  --region us-central1 \
  --entry-point insert_weather_data_to_bq \
  --trigger-topic weather-stream \
  --source streaming \
  --set-env-vars GCP_PROJECT_ID=nyc-taxi-project-477115,BQ_DATASET_ID=raw_data,BQ_TABLE_ID=weather_api_data
```

### 4.2. Deploy Taxi Functions
```bash
# Function 3: Fetch taxi trips from public dataset
gcloud functions deploy fetch-taxi-trips \
  --gen2 \
  --runtime python311 \
  --region us-central1 \
  --entry-point fetch_taxi_trips_and_publish \
  --trigger-http \
  --allow-unauthenticated \
  --source streaming \
  --set-env-vars GCP_PROJECT_ID=nyc-taxi-project-477115,TAXI_TOPIC_ID=taxi-stream,TRIPS_PER_BATCH=1000

# Function 4: Insert taxi trips to BigQuery
gcloud functions deploy insert-taxi-trips \
  --gen2 \
  --runtime python311 \
  --region us-central1 \
  --entry-point insert_taxi_trips_to_bq \
  --trigger-topic taxi-stream \
  --source streaming \
  --set-env-vars GCP_PROJECT_ID=nyc-taxi-project-477115,TAXI_DATASET_ID=streaming,TAXI_TABLE_ID=processed_trips
```

### 4.3. Setup Cloud Scheduler
```bash
# Weather scheduler (15 phút)
gcloud scheduler jobs create http weather-fetcher \
  --schedule="*/15 * * * *" \
  --uri="https://us-central1-nyc-taxi-project-477115.cloudfunctions.net/fetch-weather" \
  --location=us-central1

# Taxi scheduler (1 phút)
gcloud scheduler jobs create http taxi-fetcher \
  --schedule="* * * * *" \
  --uri="https://us-central1-nyc-taxi-project-477115.cloudfunctions.net/fetch-taxi-trips" \
  --location=us-central1
```

---

## 🎯 BƯỚC 5: Train BQML Models (sau khi có data)

### 5.1. Train Demand Forecast Model
```bash
cd ..
bq query --use_legacy_sql=false < bqml_scripts/train_model.sql
```

### 5.2. Train Fare Prediction Model
```bash
bq query --use_legacy_sql=false < bqml_scripts/train_fare_model.sql
```

### 5.3. Run Forecast
```bash
bq query --use_legacy_sql=false < bqml_scripts/run_forecast.sql
```

---

## 🎯 BƯỚC 6: Verify Setup

### 6.1. Check BigQuery Tables
```sql
-- Check dimensions
SELECT COUNT(*) FROM `nyc-taxi-project-477115.dimensions.dim_datetime`; -- Should have 365 rows
SELECT COUNT(*) FROM `nyc-taxi-project-477115.dimensions.dim_location`; -- Should have ~265 locations
SELECT COUNT(*) FROM `nyc-taxi-project-477115.dimensions.dim_weather`;  -- Should have 2-3 rows (22, 23 Nov)

-- Check streaming data
SELECT COUNT(*) FROM `nyc-taxi-project-477115.streaming.processed_trips`; -- Depends on functions
SELECT COUNT(*) FROM `nyc-taxi-project-477115.raw_data.weather_api_data`; -- Depends on functions

-- Check facts (after dbt run)
SELECT COUNT(*) FROM `nyc-taxi-project-477115.facts.fct_trips`;
SELECT COUNT(*) FROM `nyc-taxi-project-477115.facts.fct_hourly_features`;
```

### 6.2. Check dbt Models
```bash
cd nyc_taxi_pipeline
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir .
```

---

## 📝 THỨ TỰ SETUP (TÓM TẮT)

1. ✅ **Tạo BigQuery datasets & tables** (SQL commands trên)
2. ✅ **Setup Pub/Sub** (nếu dùng Cloud Functions)
3. ✅ **dbt deps** → Install packages
4. ✅ **dbt seed** → Load events calendar
5. ✅ **dbt run --select staging** → Tạo staging tables
6. ✅ **dbt run --select dimensions** → Tạo dim tables
7. ⏸️ **Deploy Cloud Functions** (optional - để có streaming data)
8. ⏸️ **Chờ streaming data accumulate** (1-2 giờ)
9. ✅ **dbt run --select facts** → Tạo fact tables (cần streaming data)
10. ✅ **Train BQML models** → Chạy ML scripts

---

## 🚨 LƯU Ý QUAN TRỌNG

### Không cần Cloud Functions để test:
- **Dimensions** (dim_datetime, dim_location, dim_weather) có thể chạy ngay vì dùng:
  - Public datasets (NOAA weather, taxi zones)
  - Seed data (events calendar)
  
- **Facts** CẦN streaming data:
  - `fct_trips` JOIN với `streaming.processed_trips`
  - Nếu không có Cloud Functions → table rỗng
  - **Workaround:** Có thể INSERT manual data vào `streaming.processed_trips` để test

### Minimum setup để test dbt:
```bash
# Chỉ cần 3 lệnh này để test dbt pipeline:
dbt deps --profiles-dir .
dbt seed --profiles-dir .
dbt run --select staging,dimensions --profiles-dir .
```

### Chi phí:
- **BigQuery storage:** FREE (dưới 10GB)
- **BigQuery queries:** FREE (dưới 1TB/tháng)
- **Cloud Functions:** FREE (trong free tier)
- **Pub/Sub:** FREE (dưới 10GB/tháng)

**Total: $0 nếu ở quy mô nhỏ!**
