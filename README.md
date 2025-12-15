# NYC Taxi Demand Forecasting

A comprehensive data engineering project that builds an end-to-end pipeline for NYC taxi demand forecasting using modern data stack: dbt, BigQuery ML, Apache Airflow, and real-time streaming capabilities.

## Project Overview

This project implements a complete data pipeline system with both batch and streaming processing:

**Batch Processing (Cold Pipeline):**
- Extracts NYC taxi trip data from BigQuery public datasets
- Integrates weather data from GSOD (Global Surface Summary of the Day)
- Incorporates NYC events and holidays calendar
- Transforms data using dbt with dimensional modeling (Star Schema)
- Trains machine learning models using BigQuery ML
- Generates demand forecasts for taxi hotspots

**Streaming Processing (Hot Pipeline):**
- Real-time weather data ingestion via Cloud Functions
- Simulated taxi trip streaming through Cloud Pub/Sub
- Real-time data processing with streaming inserts to BigQuery
- Live demand prediction API for surge pricing

**Key Capabilities:**
- 24-hour ahead demand forecasting
- 263 NYC taxi zones coverage
- Hourly prediction frequency
- H3 hexagonal grid spatial analysis
- Automated orchestration with Apache Airflow

## Architecture

The system is designed with two main processing flows:

**Data Sources:**
- BigQuery Public Dataset (NYC Taxi Trips 2022)
- Weather API (OpenWeather for real-time data)
- GSOD Weather Dataset (historical data)
- Events Calendar (US holidays and NYC events)

**Processing Layers:**
1. **Staging Layer**: Raw data cleaning and validation
2. **Dimensions**: Date, Location (H3), Weather dimensions
3. **Facts**: Trip facts and hourly aggregated features
4. **ML Layer**: Trained models and predictions
5. **Streaming Layer**: Real-time data processing

**Orchestration:**
- Apache Airflow for batch pipeline automation
- Cloud Scheduler for periodic streaming triggers
- Cloud Functions for event-driven processing

## Data Pipeline

### Staging Layer (models/staging/)
- **stg_taxi_trips**: Cleaned and validated NYC taxi trip records from public dataset
- **stg_weather**: Historical weather data with temperature conversion (Fahrenheit to Celsius)
- **stg_streaming_weather**: Real-time weather data from OpenWeather API
- **stg_weather_unified**: Combined historical and streaming weather data
- **stg_events**: NYC holidays and special events calendar

### Dimension Tables (models/marts/dimensions/)
- **dim_datetime**: Date dimension with year, month, day, day_of_week, weekend/holiday flags, event names
- **dim_location**: H3 hexagonal geospatial grid for NYC zones with zone names, borough information, and centroids
- **dim_weather**: Weather dimension with temperature, precipitation, rain/snow/fog boolean indicators

### Fact Tables (models/marts/facts/)
- **fct_trips**: Core trip facts with all dimensions joined (trip duration, distance, fare, passenger count)
- **fct_hourly_features**: ML-ready features aggregated by hour and location with lag features and rolling averages
- **fct_fare_prediction_training**: Training dataset for fare prediction models
- **fct_pca_features**: Principal Component Analysis features for clustering
- **agg_hourly_demand_h3**: Hourly demand aggregation by H3 grid for hotspot analysis

### Machine Learning Models (bqml_scripts/)
- **Boosted Tree Regressor**: Main demand forecasting model trained on hourly features
- **Fare Prediction Model**: Separate model for fare estimation

## Quick Start

### Prerequisites
- Python 3.8 or higher
- Google Cloud Platform account with billing enabled
- Google Cloud SDK installed
- dbt-bigquery package
- BigQuery API enabled

### Installation Steps

1. **Clone the repository:**
```bash
git clone https://github.com/ChienNguyen34/NYC_Taxi_Project.git
cd NYC_Taxi_Project
```

2. **Install Python dependencies:**
```bash
pip install -r requirements.txt
```

3. **Configure Google Cloud authentication:**
```bash
gcloud auth application-default login
gcloud config set project nyc-taxi-project-477115
```

4. **Set up BigQuery datasets:**
Run the setup script to create required datasets:
```bash
bq mk --dataset nyc-taxi-project-477115:staging_layer
bq mk --dataset nyc-taxi-project-477115:dimensions
bq mk --dataset nyc-taxi-project-477115:facts
bq mk --dataset nyc-taxi-project-477115:ml_models
bq mk --dataset nyc-taxi-project-477115:ml_predictions
bq mk --dataset nyc-taxi-project-477115:streaming
bq mk --dataset nyc-taxi-project-477115:raw_data
```

5. **Configure dbt profile:**
Update `~/.dbt/profiles.yml` or `nyc_taxi_pipeline/profiles.yml`:
```yaml
nyc_taxi_pipeline:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth
      project: nyc-taxi-project-477115
      dataset: staging_layer
      threads: 4
      timeout_seconds: 300
      location: US
      priority: interactive
```

6. **Install dbt packages:**
```bash
cd nyc_taxi_pipeline
dbt deps
```

7. **Load seed data and run transformations:**
```bash
dbt seed    # Load events calendar
dbt run     # Run all transformations
dbt test    # Validate data quality
```

8. **Train ML models:**
```bash
# Train the main demand forecasting model
bq query --use_legacy_sql=false < bqml_scripts/train_model.sql

# Train fare prediction model
bq query --use_legacy_sql=false < bqml_scripts/train_fare_model.sql
```

9. **Generate forecasts:**
```bash
bq query --use_legacy_sql=false < bqml_scripts/run_forecast.sql
```

## Streaming Data Setup

The project includes real-time streaming capabilities for weather data and simulated taxi trips.

### Setting up Streaming Infrastructure

1. **Create Pub/Sub topics:**
```powershell
cd streaming
.\setup_pubsub.ps1
```

2. **Configure Cloud Functions:**
Edit `streaming/main.py` environment variables or set them in deployment:
- `GCP_PROJECT_ID`: Your GCP project ID
- `OPENWEATHER_API_KEY`: Your OpenWeather API key
- `PUB_SUB_TOPIC_ID`: Topic for weather data (default: weather-stream)
- `TAXI_TOPIC_ID`: Topic for taxi data (default: taxi-stream)

3. **Deploy Cloud Functions:**
```powershell
.\deploy_functions.ps1
```

4. **Set up Cloud Scheduler:**
```powershell
.\setup_scheduler.ps1
```

### Monitoring Streaming Data

Check streaming inserts in BigQuery:
```sql
SELECT * FROM `nyc-taxi-project-477115.raw_data.weather_api_data` 
ORDER BY timestamp DESC LIMIT 100;

SELECT * FROM `nyc-taxi-project-477115.streaming.processed_trips` 
ORDER BY picked_up_at DESC LIMIT 100;
```

## Orchestration with Airflow

The project includes Apache Airflow DAG for automated pipeline execution:

**DAG Location:** `airflow_dags/nyc_taxi_dag.py`

**DAG Tasks:**
1. dbt seed - Load events calendar
2. dbt run - Execute all transformations
3. dbt test - Run data quality tests
4. Train ML model - Execute BQML training
5. Generate forecasts - Create 24h predictions

**Setup Airflow:**
1. Install Apache Airflow with Google Cloud providers
2. Copy DAG file to Airflow dags folder
3. Configure Airflow connection to BigQuery
4. Enable and trigger the DAG

## Project Structure

```
NYC_Taxi_Project/
├── README.md                       # Project documentation
├── LICENSE                         # MIT License
├── requirements.txt                # Python dependencies
├── cloudbuild.yaml                 # Cloud Build configuration
├── generate_erd.py                 # ERD generation script
├── .gitignore                      # Git ignore rules
│
├── doc/                            # Documentation
│   ├── ARCHITECTURE.md             # System architecture details
│   ├── DEPLOYMENT_GUIDE.md         # Deployment instructions
│   ├── data_warehouse_erd.md       # Data warehouse ERD
│   ├── data_warehouse_summary.md 
│   └── h3_vs_zones_comparison.md   # H3 vs traditional zones analysis
│
├── airflow_dags/                   # Apache Airflow orchestration
│   └── nyc_taxi_dag.py             # Main pipeline DAG
│
├── bqml_scripts/                   # BigQuery ML scripts
│   ├── train_model.sql             # Demand forecasting model training
│   ├── train_fare_model.sql        # Fare prediction model training
│   └── run_forecast.sql            # Generate predictions
│
├── nyc_taxi_pipeline/              # dbt project
│   ├── dbt_project.yml             # dbt configuration
│   ├── profiles.yml                # BigQuery connection profiles
│   ├── packages.yml                # dbt package dependencies
│   ├── models/                     # dbt models
│   │   ├── sources.yml             # Source definitions
│   │   ├── staging/                # Staging models
│   │   │   ├── stg_taxi_trips.sql
│   │   │   ├── stg_weather.sql
│   │   │   ├── stg_streaming_weather.sql
│   │   │   ├── stg_weather_unified.sql
│   │   │   └── stg_events.sql
│   │   └── marts/                  # Business logic models
│   │       ├── dimensions/         # Dimension tables
│   │       │   ├── dim_datetime.sql
│   │       │   ├── dim_location.sql
│   │       │   └── dim_weather.sql
│   │       └── facts/              # Fact tables
│   │           ├── fct_trips.sql
│   │           ├── fct_hourly_features.sql
│   │           ├── fct_fare_prediction_training.sql
│   │           ├── fct_pca_features.sql
│   │           └── agg_hourly_demand_h3.sql
│   ├── seeds/                      # Static data
│   │   └── events_calendar.csv     # NYC holidays and events
│   ├── tests/                      # Data quality tests
│   ├── macros/                     # dbt macros
│   │   └── get_custom_schema.sql   # Schema naming logic
│   └── dbt_packages/               # Installed dbt packages
│       └── dbt_utils/              # dbt utilities
│
├── streaming/                      # Real-time streaming
│   ├── main.py                     # Cloud Functions code
│   ├── requirements.txt            # Streaming dependencies
│   ├── setup_pubsub.ps1            # Pub/Sub setup script
│   ├── setup_pubsub.sh             # Pub/Sub setup (Linux)
│   ├── setup_scheduler.ps1         # Cloud Scheduler setup
│   └── deploy_functions.ps1       # Function deployment script
│
├── orchestration/                  # Workflow orchestration
│   ├── workflows/                  # Cloud Workflows
│   │   ├── daily_pipeline.yaml     # Daily pipeline workflow
│   │   └── deploy_workflow.ps1     # Workflow deployment
│   ├── dbt_runner/                 # dbt Cloud Run job
│   │   ├── main.py                 # dbt runner service
│   │   ├── requirements.txt
│   │   ├── deploy_run_job.ps1
│   │   └── deploy_simple.ps1
│   └── cloud_build_for_dbt_automation/
│       ├── cloudbuild.yaml         # dbt automation build
│       └── deploy_cloudbuild.ps1
│
├── dashboard/                      # Streamlit dashboard
│   ├── streamlit_dashboard.py      # Main dashboard app
│   ├── tab6_vendor_comparison.py   # Vendor analysis tab
│   ├── ml_pca_analysis.py          # PCA clustering analysis
│   ├── demo_data.py                # Demo data generator
│   ├── check_forecast_data.py      # Forecast validation
│   ├── run_demo.py                 # Demo runner
│   ├── dashboard_requirements.txt  # Dashboard dependencies
│   ├── DEMO_README.md              # Demo instructions
│   └── Dashboard_guide             # User guide
│
├── Clustering/                     # Clustering analysis (PCA, K-means)
│
├── test/                           # SQL test scripts
│   ├── setup_bigquery.sql          # BigQuery initial setup
│   ├── create_streaming_table_full.sql
│   ├── populate_streaming_trips.sql
│   ├── populate_more_trips.sql
│   ├── populate_weather_data.sql
│   └── test_columns.sql
│
└── logs/                           # Application logs
```

## Key Features

**Data Engineering:**
- **Star Schema Design**: Optimized dimensional modeling for analytical queries
- **H3 Geospatial Indexing**: Uber's H3 hexagonal grid system for uniform spatial analysis instead of irregular taxi zones
- **Incremental Processing**: Efficient data updates using dbt incremental models
- **Data Quality Testing**: Comprehensive dbt tests for data validation

**Machine Learning:**
- **BigQuery ML Integration**: Serverless ML training and inference
- **Boosted Tree Regressor**: XGBoost-based demand forecasting
- **Time Series Features**: Lag features, rolling averages, seasonal patterns
- **24-Hour Forecasts**: Hourly demand predictions for next day

**Real-Time Processing:**
- **Weather Streaming**: Live weather data ingestion via Cloud Functions
- **Pub/Sub Messaging**: Event-driven architecture for real-time data
- **Streaming Inserts**: Direct BigQuery streaming for low-latency data availability

**Automation & Orchestration:**
- **Apache Airflow**: Automated daily pipeline execution
- **Cloud Workflows**: Serverless workflow orchestration
- **Cloud Build**: CI/CD for dbt transformations
- **Cloud Scheduler**: Periodic trigger for streaming functions

**Analytics & Visualization:**
- **Streamlit Dashboard**: Interactive data exploration and visualization
- **PCA Clustering**: Zone clustering based on demand patterns
- **Vendor Comparison**: Multi-dimensional vendor performance analysis
- **Forecast Validation**: Model performance monitoring

## BigQuery Datasets Structure

## BigQuery Datasets Structure

The project creates the following BigQuery datasets:

1. **staging_layer**: Raw data after initial cleaning
   - Staging models for trips, weather, and events

2. **dimensions**: Dimension tables following star schema
   - dim_datetime, dim_location, dim_weather

3. **facts**: Fact tables and aggregations
   - fct_trips, fct_hourly_features, agg_hourly_demand_h3

4. **ml_models**: Trained BigQuery ML models
   - Demand forecasting models
   - Fare prediction models

5. **ml_predictions**: Model predictions output
   - 24-hour demand forecasts
   - Fare predictions

6. **streaming**: Real-time streaming data
   - processed_trips (streaming inserts)
   - Real-time predictions

7. **raw_data**: Raw streaming inputs
   - weather_api_data (OpenWeather API)
   - Unprocessed streaming events

## Data Quality & Testing

The project implements comprehensive data quality checks:

**dbt Tests:**
```bash
# Run all tests
dbt test

# Test specific models
dbt test --select dim_datetime
dbt test --select fct_trips
dbt test --select staging

# Test with specific tags
dbt test --select tag:quality
```

**Test Coverage:**
- Unique constraints on primary keys
- Not null checks on critical fields
- Referential integrity between facts and dimensions
- Accepted value ranges (e.g., passenger_count > 0)
- Custom business logic validations

**Validation Scripts:**
- `dashboard/check_forecast_data.py`: Validate forecast output
- `test/test_columns.sql`: Schema validation
- Data freshness checks in Airflow DAG

## Machine Learning Models

### Demand Forecasting Model

**Model Type:** Boosted Tree Regressor (XGBoost via BigQuery ML)

**Training Data:** `fct_hourly_features` table

**Features Used:**
- **Temporal Features**: hour, day_of_week, month, is_weekend, is_holiday
- **Lag Features**: pickup counts from 1 hour ago, 24 hours ago, 168 hours ago (1 week)
- **Rolling Averages**: 7-hour and 24-hour moving averages of demand
- **Weather Features**: temperature, precipitation, rain/snow/fog indicators
- **Location Features**: H3 zone, borough information

**Target Variable:** total_pickups (number of taxi trips in the next hour)

**Training Query:** See `bqml_scripts/train_model.sql`

### Fare Prediction Model

**Model Type:** Boosted Tree Regressor

**Purpose:** Predict fare amount for route planning

**Training Data:** `fct_fare_prediction_training` table

**Features:**
- Trip distance
- Pickup and dropoff H3 zones
- Time of day and day of week
- Weather conditions
- Passenger count

**Training Query:** See `bqml_scripts/train_fare_model.sql`

### Model Performance Metrics

**Demand Forecasting:**
- **MAPE (Mean Absolute Percentage Error)**: 15-20%
- **Coverage**: 263 NYC taxi zones
- **Prediction Frequency**: Hourly
- **Forecast Horizon**: 24 hours ahead
- **Training Data**: 2022 NYC Yellow Taxi trips (~40 million records)

**Performance Notes:**
- Higher accuracy during weekdays vs weekends
- Better performance in high-demand zones (Manhattan)
- Weather events can increase prediction error
- Holiday periods show higher variability

## Dashboard & Visualization

The project includes an interactive Streamlit dashboard for data exploration and monitoring.

**Features:**
- **Demand Heatmaps**: Geographic visualization of taxi demand
- **Time Series Plots**: Hourly, daily, weekly demand patterns
- **Forecast Comparison**: Actual vs predicted demand
- **Customer segmentation & RFM analysis**: Customer segmentation based on RFM, focast revenue for each locations.
- **Vendor Analysis**: Performance comparison across vendors
- **PCA Clustering**: Zone clustering based on demand patterns

**Running the Dashboard:**
```bash
cd dashboard
pip install -r dashboard_requirements.txt
streamlit run streamlit_dashboard.py
```

**Demo Mode:**
```bash
# Generate demo data
python demo_data.py

# Run demo dashboard
python run_demo.py
```

See `dashboard/DEMO_README.md` for detailed instructions.

## Deployment

### Cloud Build Automation

Deploy automated dbt runs:
```powershell
cd orchestration/cloud_build_for_dbt_automation
.\deploy_cloudbuild.ps1
```

### Cloud Workflows

Deploy daily pipeline workflow:
```powershell
cd orchestration/workflows
.\deploy_workflow.ps1
```

### dbt Cloud Run Service

Deploy dbt as a Cloud Run service:
```powershell
cd orchestration/dbt_runner
.\deploy_simple.ps1
```

## Documentation

Comprehensive documentation is available in the `doc/` directory:

- **ARCHITECTURE.md**: Detailed system architecture with diagrams
- **DEPLOYMENT_GUIDE.md**: Step-by-step deployment instructions
- **data_warehouse_erd.md**: Entity Relationship Diagram
- **h3_vs_zones_comparison.md**: H3 vs traditional zones analysis

## Technology Stack

**Data Platform:**
- Google BigQuery (Data Warehouse)
- Google Cloud Storage (Data Lake)
- Cloud Pub/Sub (Message Queue)
- Cloud Functions (Serverless Computing)

**Data Transformation:**
- dbt (Data Build Tool)
- SQL (BigQuery SQL)

**Machine Learning:**
- BigQuery ML
- XGBoost (Boosted Tree Regressor)

**Orchestration:**
- Apache Airflow
- Cloud Workflows
- Cloud Scheduler
- Cloud Build

**Languages:**
- Python 3.8+
- SQL
- YAML

**Visualization:**
- Streamlit
- Plotly
- Matplotlib

**Geospatial:**
- H3 (Uber's Hexagonal Hierarchical Spatial Index)
- GeoPandas

## Cost Optimization

The project implements several cost optimization strategies:

1. **Partitioning**: Large tables partitioned by date
2. **Clustering**: Tables clustered by frequently filtered columns
3. **Views vs Tables**: Use views for large intermediate results
4. **Incremental Models**: Process only new/changed data
5. **Query Optimization**: Avoid SELECT *, use specific columns
6. **Streaming Batching**: Batch streaming inserts to reduce costs
7. **BigQuery ML**: Serverless ML eliminates compute infrastructure costs

## Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/your-feature-name`
3. Make your changes with clear, descriptive commits
4. Run tests to ensure data quality: `dbt test`
5. Update documentation if needed
6. Commit your changes: `git commit -m 'Add feature: description'`
7. Push to your branch: `git push origin feature/your-feature-name`
8. Open a Pull Request with detailed description

**Code Standards:**
- Follow PEP 8 for Python code
- Use meaningful variable and function names
- Add comments for complex logic
- Include docstrings for functions
- Test your SQL queries before committing

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

**Data Sources:**
- NYC Taxi & Limousine Commission for providing public taxi trip data
- NOAA GSOD for historical weather data
- OpenWeather API for real-time weather data

**Technologies:**
- dbt Labs for the excellent data transformation framework
- Google Cloud Platform for BigQuery ML and cloud infrastructure
- Uber Engineering for the H3 geospatial indexing system
- Apache Software Foundation for Apache Airflow

**Special Thanks:**
- NYC Open Data initiative
- BigQuery public datasets program
- The open-source data engineering community

## Contact & Support

**Project Author:** ChienNguyen34

**Email:** chiennguyen.developer@gmail.com

**Project Repository:** https://github.com/ChienNguyen34/NYC_Taxi_Project

**Issues & Questions:** Please open an issue on GitHub for:
- Bug reports
- Feature requests
- Questions about setup or usage
- Documentation improvements

**Project Status:** Active development and maintenance

**Last Updated:** December 2025