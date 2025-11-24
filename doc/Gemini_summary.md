graph TD
    subgraph "Điều phối (Orchestrator): Airflow (Cloud Composer)"
    
    direction TD

    %% Giai đoạn 1: Staging (dbt)
    subgraph "Giai đoạn 1: Staging"
        direction LR
        BQ_Public_Taxi[("💾 BQ Public: NYC Taxi")]
        BQ_Public_Weather[("🌦️ BQ Public: NOAA Weather")]
        GCS_Events[("📅 GCS: lich_su_kien.csv")]
        
        Tool_dbt1(Tool: dbt Core)

        Stg_Tables[("Staging Tables<br>stg_taxi_trips<br>stg_weather<br>stg_events")]

        BQ_Public_Taxi --> Tool_dbt1
        BQ_Public_Weather --> Tool_dbt1
        GCS_Events --> Tool_dbt1
        Tool_dbt1 --> Stg_Tables
    end

    %% Giai đoạn 2: Transformation (dbt)
    subgraph "Giai đoạn 2: Transformation (Data Model)"
        Tool_dbt2(Tool: dbt Core)
        Star_Schema[("Star Schema Tables<br>dim_datetime<br>dim_location<br>dim_weather<br>fct_trips")]
        Stg_Tables --> Tool_dbt2 --> Star_Schema
    end

    %% Giai đoạn 3: Aggregation (dbt)
    subgraph "Giai đoạn 3: Aggregation (Feature Engineering)"
        Tool_dbt3(Tool: dbt Core)
        Agg_Table[("Feature Table<br>agg_hourly_demand_features")]
        Star_Schema --> Tool_dbt3 --> Agg_Table
    end

    %% Giai đoạn 4: Machine Learning (BQML)
    subgraph "Giai đoạn 4: Machine Learning"
        Tool_BQML(Tool: BigQuery ML)
        ML_Model[("🤖 BQML Model<br>timeseries_hotspot_model")]
        Prediction_Table[("🔮 Prediction Table<br>predictions.hourly_hotspots")]
        
        Agg_Table --> Tool_BQML
        Tool_BQML --> ML_Model
        Tool_BQML --> Prediction_Table
    end

    %% Giai đoạn 5: Serving & Visualization
    subgraph "Giai đoạn 5: Serving & Visualization"
        direction LR
        Dashboard[("📈 Dashboard<br>Looker Studio")]
        API[("☁️ API<br>Cloud Function")]
        
        Prediction_Table --> Dashboard
        Prediction_Table --> API
    end

    %% Định nghĩa luồng chính của Airflow
    Giai_doan_1["(Task: dbt run - Staging)"]
    Giai_doan_2["(Task: dbt run - Marts)"]
    Giai_doan_3["(Task: dbt run - Agg)"]
    Giai_doan_4["(Task: BQML Train & Forecast)"]
    
    Staging --> Transformation
    Transformation --> Aggregation
    Aggregation --> Machine_Learning
    Machine_Learning --> Serving_Visualization
    
    end

    %% Style (Tùy chọn)
    style Giai_doan_1 fill:#f9f,stroke:#333,stroke-width:2px
    style Giai_doan_2 fill:#f9f,stroke:#333,stroke-width:2px
    style Giai_doan_3 fill:#f9f,stroke:#333,stroke-width:2px
    style Giai_doan_4 fill:#ccf,stroke:#333,stroke-width:2px
