# NYC Taxi Data Warehouse - Entity Relationship Diagram

This diagram shows the relationships between tables in the data warehouse.

## Star Schema Architecture

- **Fact Tables**: `fct_trips`, `fct_hourly_features`, `fct_fare_prediction_training`
- **Dimension Tables**: `dim_datetime`, `dim_location`, `dim_weather`
- **ML Predictions**: `hourly_demand_forecast`

```mermaid
erDiagram
    dim_datetime {
        string date_id PK
        date full_date
        int year
        int month
        int day
        int day_of_week
        int day_of_year
        int quarter
        bool is_weekend
        string event_name
        string event_type
        bool is_holiday
    }
    dim_location {
        string zone_id
        string zone_name
        string borough
        GEOGRAPHY zone_centroid
        float h3_centroid_longitude
        float h3_centroid_latitude
        string h3_id PK
    }
    dim_weather {
        date weather_date PK
        decimal avg_temp_celsius
        decimal max_temp_celsius
        decimal min_temp_celsius
        decimal total_precipitation_mm
        bool had_rain
        bool had_snow
        bool had_fog
    }
    fct_trips {
        string trip_id PK
        string vendor_id
        string payment_type_id
        string rate_code_id
        string datetime_id FK
        string pickup_h3_id FK
        string dropoff_h3_id FK
        date weather_date PK
        timestamp picked_up_at FK
        timestamp dropped_off_at FK
        int passenger_count
        decimal trip_distance
        int trip_duration_seconds
        decimal fare_amount
        decimal extra_amount
        decimal mta_tax
        decimal tip_amount
        decimal tolls_amount
        decimal improvement_surcharge
        decimal airport_fee
        decimal total_amount
    }
    fct_hourly_features {
        int total_pickups
        string pickup_h3_id FK
        timestamp timestamp_hour PK
        int hour_of_day
        int day_of_week
        int month
        int quarter
        int day_of_year
        decimal avg_temp_celsius
        decimal total_precipitation_mm
        bool had_rain
        bool had_snow
        bool is_weekend
        bool is_holiday
        int pickups_1h_ago
        int pickups_24h_ago
        int pickups_1week_ago
        float avg_pickups_7h
        float avg_pickups_24h
        int pickups_change_24h
        int rain_during_rush_hour
    }
    fct_fare_prediction_training {
        float fare_amount
        int passenger_count
        decimal trip_distance
        int trip_duration_seconds
        int hour_of_day
        int day_of_week
        bool is_holiday
        bool is_weekend
        string pickup_h3_id FK
        string dropoff_h3_id FK
        decimal avg_temp_celsius
        decimal total_precipitation_mm
        bool had_rain
        bool had_snow
        int historical_demand
    }
    hourly_demand_forecast {
        timestamp timestamp_hour PK
        string pickup_h3_id FK
        int actual_pickups
        float predicted_total_pickups
        decimal avg_temp_celsius
        bool had_rain
        bool is_weekend
        bool is_holiday
    }
    dim_datetime ||--o{ fct_trips : "datetime_key to picked_up_at"
    dim_location ||--o{ fct_trips : "h3_id to pickup_h3_id"
    dim_location ||--o{ fct_trips : "h3_id to dropoff_h3_id"
    dim_location ||--o{ fct_hourly_features : "h3_id to pickup_h3_id"
    dim_datetime ||--o{ fct_hourly_features : "datetime_key to timestamp_hour"
    dim_weather ||--o{ fct_hourly_features : "datetime_hour to timestamp_hour"
    fct_hourly_features ||--o{ hourly_demand_forecast : "timestamp_hour to timestamp_hour"
    fct_hourly_features ||--o{ hourly_demand_forecast : "pickup_h3_id to pickup_h3_id"
```
