# Hệ thống Phân tích và Dự báo Hotspot Nhu cầu Taxi theo Thời gian thực và Đề xuất Giá Động

## 🎯 Tổng quan Dự án

### Mục tiêu chính
- Xây dựng pipeline dự báo nhu cầu taxi trong tương lai gần (1-24 giờ tới)
- Phân tích theo không gian địa lý cụ thể (H3 hexagonal cells)
- Mô phỏng hệ thống đề xuất giá động (surge pricing)
- Áp dụng Modern Data Stack (BigQuery, dbt, Airflow, BQML)

### Điểm "Wow" của dự án
✅ **Vượt ngoài BI truyền thống**: Tạo mô hình dự báo + API serving  
✅ **Data Enrichment**: Kết hợp taxi + weather + events data  
✅ **Geospatial Analysis**: Sử dụng H3 hexagonal grid  
✅ **Modern Data Stack**: Full pipeline với dbt, Airflow, BQML  

---

## 🏗️ Kiến trúc Hệ thống

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Data Sources  │────│   BigQuery DW    │────│  Transformation │
│                 │    │                  │    │      (dbt)      │
│ • NYC Taxi      │    │ • Raw Tables     │    │                 │
│ • Weather       │    │ • Staging        │    │ • Staging       │
│ • Events        │    │ • Marts          │    │ • Dimensions    │
└─────────────────┘    └──────────────────┘    │ • Facts         │
                                               └─────────────────┘
                              │
                              ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│  Orchestration  │────│   ML Training    │────│   Serving       │
│   (Airflow)     │    │     (BQML)       │    │                 │
│                 │    │                  │    │ • Dashboard     │
│ • Schedule      │    │ • Time Series    │    │ • API           │
│ • Monitor       │    │ • Forecasting    │    │ • Predictions   │
│ • Test          │    │ • Predictions    │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

---

## 📊 Nguồn Dữ liệu

### 1. NYC Taxi Trips
```sql
-- Source: bigquery-public-data.new_york_taxi_trips.tlc_*_trips_*
-- Chứa: pickup/dropoff location, timestamp, fare, distance
```

### 2. Weather Data
```sql
-- Source: bigquery-public-data.noaa_gsod.gsod*
-- Chứa: temperature, precipitation, wind_speed
```

### 3. Events Calendar (Custom)
```sql
-- Custom CSV upload to GCS -> BigQuery
-- Chứa: holiday_name, date, event_type
```

---

## 🔄 Data Pipeline Flow

### Phase 1: Data Ingestion
```
Raw Data Sources → BigQuery Raw Tables
```

### Phase 2: Data Transformation (dbt)

#### Staging Layer
```sql
-- stg_taxi_trips: Clean & standardize taxi data
-- stg_weather: Filter weather data for NYC
-- stg_events: Load events calendar
```

#### Dimensions Layer
```sql
-- dim_datetime: Time dimension (hour, day, is_holiday, is_rush_hour)
-- dim_location_h3: Geospatial dimension using H3 hexagonal cells
```

#### Facts Layer
```sql
-- fct_trips: Main fact table joining all sources
-- agg_hourly_demand_h3: Aggregated hourly demand by H3 cell
```

### Phase 3: Feature Engineering
```sql
-- Target: total_pickups per H3 cell per hour
-- Features: temperature, precipitation, is_holiday, is_rush_hour, etc.
```

### Phase 4: ML Training & Prediction (BQML)
```sql
-- CREATE MODEL: TIME_SERIES_FORECASTING (ARIMA_PLUS/Prophet)
-- ML.FORECAST: Generate 24-hour predictions
```

### Phase 5: Serving & Visualization
```sql
-- Dashboard: Heatmap with time slider
-- API: Real-time surge pricing recommendations
```

---

## 🗄️ Data Model

### Core Tables Structure

```
📁 Raw Layer
├── taxi_trips_raw
├── weather_raw
└── events_raw

📁 Staging Layer (dbt models)
├── stg_taxi_trips
├── stg_weather
└── stg_events

📁 Dimensions (dbt models)
├── dim_datetime
└── dim_location_h3

📁 Facts (dbt models)
├── fct_trips
└── agg_hourly_demand_h3

📁 ML & Predictions
├── ml_model_demand_forecast
└── predictions_hourly_hotspots

📁 Business Rules
└── rules_surge_pricing
```

### Key Table Schemas

#### `agg_hourly_demand_h3` (Main Feature Table)
```sql
├── h3_cell_id (STRING)           -- H3 hexagonal cell identifier
├── timestamp_hour (TIMESTAMP)    -- Hour timestamp
├── total_pickups (INT64)         -- Target variable
├── avg_temperature (FLOAT64)     -- Weather feature
├── total_precipitation (FLOAT64) -- Weather feature
├── is_holiday (BOOL)             -- Calendar feature
├── is_rush_hour (BOOL)           -- Time feature
├── day_of_week (INT64)           -- Time feature
└── hour_of_day (INT64)           -- Time feature
```

#### `predictions_hourly_hotspots` (ML Output)
```sql
├── h3_cell_id (STRING)
├── forecast_timestamp (TIMESTAMP)
├── predicted_demand (FLOAT64)
├── prediction_interval_lower (FLOAT64)
├── prediction_interval_upper (FLOAT64)
└── model_version (STRING)
```

#### `rules_surge_pricing` (Business Logic)
```sql
├── demand_threshold_min (INT64)
├── demand_threshold_max (INT64)
├── surge_multiplier (FLOAT64)
└── rule_description (STRING)
```

---

## 🛠️ Tech Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Cloud Platform** | Google Cloud Platform | Infrastructure |
| **Data Warehouse** | BigQuery | Storage & Compute |
| **Data Transformation** | dbt | SQL modeling & testing |
| **Orchestration** | Cloud Composer (Airflow) | Pipeline scheduling |
| **Machine Learning** | BigQuery ML | Time series forecasting |
| **Visualization** | Looker Studio | Dashboard & reporting |
| **API Serving** | Cloud Functions/Run | Real-time predictions |

---

## 📅 Airflow DAG Workflow

```python
# Daily Pipeline Schedule
dag_taxi_demand_forecast = DAG(
    'taxi_demand_forecast',
    schedule_interval='0 1 * * *',  # Daily at 1 AM
    tasks=[
        dbt_run_staging,      # Transform raw data
        dbt_run_marts,        # Build dimensions & facts
        dbt_test_all,         # Data quality tests
        bqml_retrain_model,   # Update ML model
        bqml_generate_forecast, # Create predictions
        update_dashboard,     # Refresh visualizations
    ]
)
```

---

## 📈 Expected Outcomes

### 1. Predictive Dashboard
- Interactive NYC heatmap showing predicted demand hotspots
- Time slider for 24-hour forecast horizon
- Comparison charts: predicted vs actual demand

### 2. Surge Pricing API
```json
// API Response Example
{
  "location": {
    "latitude": 40.7128,
    "longitude": -74.0060,
    "h3_cell_id": "8a2a1072b59ffff"
  },
  "prediction": {
    "timestamp": "2025-11-03T15:00:00Z",
    "predicted_demand": 150,
    "confidence_interval": [130, 170]
  },
  "pricing": {
    "base_fare": 2.50,
    "surge_multiplier": 1.5,
    "suggested_fare": 3.75
  }
}
```

### 3. Business Intelligence
- Identification of temporal patterns (rush hours, weekends, holidays)
- Weather impact analysis on taxi demand
- Geographic demand distribution insights

---

## 🎯 Project Value & Innovation

### Academic Merit
- **Data Engineering Excellence**: Full modern data stack implementation
- **Geospatial Analytics**: Advanced H3 hexagonal grid analysis
- **Real-time ML**: Time series forecasting with BQML
- **End-to-end Pipeline**: From raw data to business application

### Business Impact
- **Operational Efficiency**: Optimized taxi fleet positioning
- **Revenue Optimization**: Dynamic pricing based on predicted demand
- **Customer Experience**: Reduced wait times through demand forecasting
- **Data-Driven Decisions**: Evidence-based resource allocation

---

## 📋 Implementation Checklist

- [ ] Set up GCP project and BigQuery datasets
- [ ] Configure dbt project structure
- [ ] Implement staging models for data cleaning
- [ ] Build dimension and fact tables
- [ ] Create feature engineering pipeline
- [ ] Train BQML forecasting model
- [ ] Set up Airflow DAG for orchestration
- [ ] Build Looker Studio dashboard
- [ ] Develop surge pricing API
- [ ] Implement data quality tests
- [ ] Document and test entire pipeline

---

*This project represents a comprehensive Modern Data Engineering solution, combining multiple data sources, advanced analytics, machine learning, and real-time serving capabilities.*