# 🚕 NYC Taxi Demand Forecasting

Real-time taxi demand forecasting system using modern data stack: dbt, BigQuery ML, and Apache Airflow.

## 🎯 Project Overview

This project implements an end-to-end data pipeline that:
- **Extracts** NYC taxi trip data and weather information
- **Transforms** data using dbt with dimensional modeling
- **Loads** clean data into BigQuery
- **Trains** ARIMA_PLUS time series models using BigQuery ML
- **Generates** 24-hour demand forecasts
- **Orchestrates** the entire pipeline with Apache Airflow

## 🏗️ Architecture

```
NYC Taxi Data (BigQuery Public Dataset)
           ↓
    dbt Transformations
    ├── Staging Layer
    ├── Dimension Tables  
    └── Fact Tables
           ↓
    BigQuery ML (ARIMA_PLUS)
           ↓
    Demand Forecasts
           ↓
    Visualization & API
```

## 📊 Data Pipeline

### Staging Layer
- `stg_taxi_trips`: Clean taxi trip data
- `stg_weather`: Weather data with temperature conversion
- `stg_events`: NYC holidays and events calendar

### Dimension Tables  
- `dim_datetime`: Date dimension with holiday flags
- `dim_location`: H3 geospatial grid for NYC zones
- `dim_weather`: Weather dimension with boolean flags

### Fact Tables
- `fct_trips`: Core trip facts with all dimensions joined
- `fct_hourly_features`: ML-ready features aggregated by hour and location

### ML Models
- `timeseries_hotspot_model`: ARIMA_PLUS model for demand forecasting

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- dbt-bigquery
- Google Cloud SDK
- BigQuery project with billing enabled

### Installation

1. Clone the repository:
```bash
git clone https://github.com/[YOUR-USERNAME]/NYC-Taxi-Demand-Forecasting.git
cd NYC-Taxi-Demand-Forecasting
```

2. Install dependencies:
```bash
pip install dbt-bigquery
```

3. Configure dbt profiles:
```bash
dbt init
# Configure BigQuery connection in ~/.dbt/profiles.yml
```

4. Install dbt packages:
```bash
cd nyc_taxi_pipeline
dbt deps
```

5. Run the pipeline:
```bash
dbt seed  # Load events calendar
dbt run   # Run all transformations
dbt test  # Validate data quality
```

6. Train ML model:
```bash
bq query --use_legacy_sql=false < bqml_scripts/train_model.sql
```

7. Generate forecasts:
```bash
bq query --use_legacy_sql=false < bqml_scripts/run_forecast.sql
```

## 🌊 Streaming Data Simulation

For testing the real-time pipeline without actual mobile apps:

```bash
# Setup streaming infrastructure
cd streaming_simulation
python setup_streaming.py

# Run simulation (converts historical data to real-time events)
python simulate_realtime_taxi_data.py

# Monitor streaming events (in another terminal)
python monitor_stream.py

# Run end-to-end tests
python test_e2e.py
```

See `streaming_simulation/README.md` for detailed instructions.

## 📁 Project Structure

```
NYC_Taxi_Project/
├── README.md                    # This file
├── .gitignore                   # Git ignore rules
├── requirements.txt             # Python dependencies
├── ARCHITECTURE.md              # System architecture documentation
├── airflow_dags/               # Airflow orchestration
│   └── nyc_taxi_dag.py
├── bqml_scripts/               # BigQuery ML scripts
│   ├── train_model.sql
│   └── run_forecast.sql
├── streaming_simulation/        # Real-time streaming simulation
│   ├── README.md               # Streaming setup instructions
│   ├── simulate_realtime_taxi_data.py  # Main simulation script
│   ├── setup_streaming.py      # Infrastructure setup
│   ├── monitor_stream.py       # Real-time monitoring
│   ├── test_e2e.py            # End-to-end testing
│   └── requirements.txt        # Streaming dependencies
└── nyc_taxi_pipeline/          # dbt project
    ├── dbt_project.yml
    ├── models/
    │   ├── staging/            # Raw data transformations
    │   └── marts/
    │       ├── dimensions/     # Dimension tables
    │       └── facts/          # Fact tables
    ├── seeds/                  # Static data files
    ├── tests/                  # Data quality tests
    └── macros/                 # dbt macros
```

## 🔧 Configuration

### BigQuery Setup
1. Create a BigQuery project
2. Enable BigQuery API
3. Create datasets: `staging_layer`, `dimensions`, `facts`, `ml_models`, `ml_predictions`
4. Configure authentication (service account or OAuth)

### dbt Configuration
Update `profiles.yml`:
```yaml
nyc_taxi_pipeline:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth
      project: [YOUR-PROJECT-ID]
      dataset: staging_layer
      threads: 4
      timeout_seconds: 300
      location: US
```

## 📈 Features

- **Geospatial Analysis**: H3 hexagonal grid for uniform spatial analysis
- **Weather Integration**: Temperature, precipitation, and weather conditions
- **Holiday Effects**: Automatic US holiday detection and modeling
- **Time Series Forecasting**: 24-hour ahead demand predictions
- **Data Quality Testing**: Comprehensive dbt tests
- **Cost Optimization**: Views for large tables, tables for small dimensions

## 🧪 Testing

Run data quality tests:
```bash
dbt test
```

Test specific models:
```bash
dbt test --select dim_datetime
dbt test --select fct_trips
```

## 📊 Model Performance

The ARIMA_PLUS model achieves:
- **MAPE**: ~15-20% (typical for taxi demand forecasting)
- **Coverage**: 263 NYC taxi zones
- **Frequency**: Hourly predictions
- **Horizon**: 24 hours ahead

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Make your changes
4. Run tests: `dbt test`
5. Commit your changes: `git commit -m 'Add amazing feature'`
6. Push to the branch: `git push origin feature/amazing-feature`
7. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- NYC Taxi & Limousine Commission for public data
- dbt Labs for the amazing transformation framework
- Google Cloud for BigQuery ML capabilities
- H3 geospatial indexing system by Uber

## 📧 Contact

- **Author**: ChienNguyen34
- **Email**: chiennguyen.developer@gmail.com
- **Project Link**: https://github.com/[YOUR-USERNAME]/NYC-Taxi-Demand-Forecasting