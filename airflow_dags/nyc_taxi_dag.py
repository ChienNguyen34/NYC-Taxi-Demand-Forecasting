# airflow_dags/nyc_taxi_dag.py
# (FIXED: Đã cập nhật để đọc SQL từ file với BigQueryInsertJobOperator)

import os
from datetime import datetime
from airflow.decorators import dag
from airflow.models.variable import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# --- Helper function để đọc SQL files ---
def read_sql_file(file_path):
    """Read SQL file content"""
    try:
        with open(file_path, 'r') as file:
            return file.read()
    except FileNotFoundError:
        # Fallback for Cloud Composer environment
        return f"-- File not found: {file_path}"

# --- Biến Cấu hình ---
# Lấy Project ID từ kết nối Airflow
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT", "nyc-taxi-project-477115")

# Lấy đường dẫn GỐC của thư mục dags
# Chúng ta sẽ cài đặt biến 'DAGS_FOLDER_PATH' này trong Airflow UI
# Giá trị: /home/airflow/gcs/dags
DAGS_FOLDER_PATH = Variable.get("DAGS_FOLDER_PATH", "/home/airflow/gcs/dags")

# Xác định đường dẫn cho các dự án con
DBT_PROJECT_DIR = f"{DAGS_FOLDER_PATH}/nyc_taxi_pipeline"
BQML_SCRIPT_DIR = f"{DAGS_FOLDER_PATH}/bqml_scripts"

# --- Định nghĩa DAG ---

@dag(
    dag_id='nyc-taxi-orchestrator',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily', # Chạy hàng ngày
    catchup=False,
    tags=['nyc-taxi', 'dbt', 'bqml'],
)
def nyc_taxi_pipeline():
    """
    DAG tự động hóa toàn bộ pipeline: dbt -> BQML -> Predictions.
    Đọc SQL từ các file riêng biệt để dễ quản lý.
    """

    # Task 1: Setup và cài đặt thư viện dbt-utils
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command=f'''
        mkdir -p /home/airflow/.dbt
        cd {DBT_PROJECT_DIR}
        cp profiles.yml /home/airflow/.dbt/profiles.yml
        dbt deps
        '''
    )

    # Task 2: Chạy dbt seed (load file events_calendar.csv)
    dbt_seed = BashOperator(
        task_id='dbt_seed',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt seed'
    )

    # Task 3: Chạy tất cả model dbt (staging, dims, facts)
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run'
    )

    # Task 4: Chạy dbt test (kiểm tra chất lượng dữ liệu)
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt test',
        trigger_rule='all_done' 
    )

    # Task 5: Huấn luyện model BQML
    bqml_train = BigQueryInsertJobOperator(
        task_id='bqml_train_model',
        configuration={
            "query": {
                "query": f"""
                CREATE OR REPLACE MODEL `{GCP_PROJECT_ID}.ml_models.timeseries_hotspot_model`
                OPTIONS(
                    model_type='ARIMA_PLUS',
                    time_series_timestamp_col='timestamp_hour',
                    time_series_data_col='total_pickups',
                    time_series_id_col='pickup_h3_id',
                    data_frequency='HOURLY',
                    holiday_region='US'
                )
                AS
                SELECT
                    timestamp_hour,
                    pickup_h3_id,
                    total_pickups
                FROM
                    `{GCP_PROJECT_ID}.facts.fct_hourly_features`
                """,
                "useLegacySql": False,
            }
        },
        gcp_conn_id='google_cloud_default',
        location='US'
    )

    # Task 6: Chạy dự báo
    bqml_forecast = BigQueryInsertJobOperator(
        task_id='bqml_run_forecast',
        configuration={
            "query": {
                "query": f"""
                CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.ml_predictions.hourly_demand_forecast`
                AS
                SELECT *
                FROM
                  ML.FORECAST(
                    MODEL `{GCP_PROJECT_ID}.ml_models.timeseries_hotspot_model`,
                    STRUCT(
                        24 AS horizon,
                        0.8 AS confidence_level
                    )
                  )
                """,
                "useLegacySql": False,
            }
        },
        gcp_conn_id='google_cloud_default',
        location='US'
    )

    # --- Sắp xếp thứ tự các Task ---
    dbt_deps >> dbt_seed >> dbt_run >> dbt_test >> bqml_train >> bqml_forecast

# Gọi hàm để tạo DAG
nyc_taxi_pipeline()