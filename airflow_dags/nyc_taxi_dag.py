# airflow_dags/nyc_taxi_dag.py
# (FIXED: Đã cập nhật để đọc SQL từ file với BigQueryInsertJobOperator)
# (UPDATED: Thêm pipeline huấn luyện model dự đoán giá cước)

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
        # In a real scenario, you'd want more robust error handling
        return f"-- SQL file not found at {file_path}"

# --- Biến Cấu hình ---
# Lấy Project ID từ kết nối Airflow hoặc biến môi trường
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
    template_searchpath=DAGS_FOLDER_PATH # Cho phép Airflow tìm SQL files
)
def nyc_taxi_pipeline():
    """
    DAG tự động hóa toàn bộ pipeline:
    1. dbt: Build các data model (staging, dimensions, facts).
    2. BQML (Demand Forecast): Huấn luyện model dự báo nhu cầu.
    3. BQML (Fare Prediction): Huấn luyện model dự đoán giá cước.
    """

    # === DBT TASKS ===
    # Task 1: Cài đặt thư viện dbt
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps"
    )

    # Task 2: Chạy dbt seed (load file events_calendar.csv)
    dbt_seed = BashOperator(
        task_id='dbt_seed',
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt seed"
    )

    # Task 3: Chạy tất cả model dbt (staging, dims, facts)
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run"
    )

    # Task 4: Chạy dbt test (kiểm tra chất lượng dữ liệu)
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test",
        trigger_rule='all_done'
    )

    # === BQML TASKS ===

    # --- Pipeline 1: Demand Forecasting ---

    # Task 5a: Huấn luyện model dự báo nhu cầu (Demand Forecast)
    bqml_train_demand = BigQueryInsertJobOperator(
        task_id='bqml_train_demand_model',
        configuration={
            "query": {
                "query": read_sql_file(f"{BQML_SCRIPT_DIR}/train_model.sql").replace('{{ var.gcp_project_id }}', GCP_PROJECT_ID),
                "useLegacySql": False,
            }
        },
        gcp_conn_id='google_cloud_default',
        location='US'
    )

    # Task 6a: Chạy dự báo nhu cầu
    bqml_forecast_demand = BigQueryInsertJobOperator(
        task_id='bqml_run_demand_forecast',
        configuration={
            "query": {
                "query": read_sql_file(f"{BQML_SCRIPT_DIR}/run_forecast.sql").replace('{{ var.gcp_project_id }}', GCP_PROJECT_ID),
                "useLegacySql": False,
            }
        },
        gcp_conn_id='google_cloud_default',
        location='US'
    )

    # --- Pipeline 2: Fare Prediction ---

    # Task 5b: Huấn luyện model dự đoán giá cước (Fare Prediction)
    bqml_train_fare = BigQueryInsertJobOperator(
        task_id='bqml_train_fare_model',
        configuration={
            "query": {
                "query": read_sql_file(f"{BQML_SCRIPT_DIR}/train_fare_model.sql").replace('{{ var.gcp_project_id }}', GCP_PROJECT_ID),
                "useLegacySql": False,
            }
        },
        gcp_conn_id='google_cloud_default',
        location='US'
    )

    # --- Sắp xếp thứ tự các Task ---
    # dbt tasks run sequentially
    dbt_deps >> dbt_seed >> dbt_run >> dbt_test

    # BQML tasks run after dbt tests are done
    dbt_test >> bqml_train_demand
    dbt_test >> bqml_train_fare

    # The demand forecast runs after the demand model is trained
    bqml_train_demand >> bqml_forecast_demand

# Gọi hàm để tạo DAG
nyc_taxi_pipeline()
