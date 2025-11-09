# airflow_dags/nyc_taxi_dag.py
# (FIXED: Đã cập nhật để đọc SQL từ file, thay vì nhúng trực tiếp)

import os
from datetime import datetime
from airflow.decorators import dag
from airflow.models.variable import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

# --- Biến Cấu hình ---
# Lấy Project ID từ kết nối Airflow
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT")

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

    # Task 1: Cài đặt thư viện dbt-utils
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

    # Task 5: Huấn luyện model BQML
    # (FIXED: đọc SQL từ file)
    bqml_train = BigQueryExecuteQueryOperator(
        task_id='bqml_train_model',
        sql=f"{BQML_SCRIPT_DIR}/train_model.sql", # Đường dẫn đến file SQL
        use_legacy_sql=False,
        gcp_conn_id='google_cloud_default'
    )

    # Task 6: Chạy dự báo
    # (FIXED: đọc SQL từ file)
    bqml_forecast = BigQueryExecuteQueryOperator(
        task_id='bqml_run_forecast',
        sql=f"{BQML_SCRIPT_DIR}/run_forecast.sql", # Đường dẫn đến file SQL
        use_legacy_sql=False,
        gcp_conn_id='google_cloud_default'
    )

    # --- Sắp xếp thứ tự các Task ---
    dbt_deps >> dbt_seed >> dbt_run >> dbt_test >> bqml_train >> bqml_forecast

# Gọi hàm để tạo DAG
nyc_taxi_pipeline()