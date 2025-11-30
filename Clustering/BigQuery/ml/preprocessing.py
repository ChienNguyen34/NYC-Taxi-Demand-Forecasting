import pandas as pd
from google.cloud import bigquery
from utils.bq_client import query_to_dataframe


def load_ml_dataset(client: bigquery.Client) -> pd.DataFrame:
    """
    Đọc dữ liệu từ bảng fact.ml_dataset.
    """
    TABLE_ID = "nyc-taxi-project-479008.fact.ml_dataset"

    query = f"""
    SELECT
        zone_id,
        pickup_zone_name,
        total_trips,
        avg_hourly_demand,
        trips_per_km2,
        weekend_ratio
    FROM `{TABLE_ID}`
    """

    df = query_to_dataframe(query, client)
    print("📥 Loaded ML dataset, rows:", df.shape[0])
    return df


def drop_null_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Loại bỏ các dòng NULL trong những cột quan trọng.
    """
    cols_required = [
        "zone_id",
        "pickup_zone_name",
        "total_trips",
        "avg_hourly_demand",
        "trips_per_km2",
        "weekend_ratio"
    ]

    before = df.shape[0]
    df_clean = df.dropna(subset=cols_required)
    after = df_clean.shape[0]

    print(f"🧹 Drop NULL: {before} → {after} rows")
    return df_clean


def preprocess_ml_dataset(client: bigquery.Client) -> pd.DataFrame:
    """
    Hàm tổng hợp:
    1. đọc bảng ml_dataset
    2. loại bỏ NULL
    3. trả về dataframe sạch
    """
    df = load_ml_dataset(client)
    df = drop_null_rows(df)
    return df
