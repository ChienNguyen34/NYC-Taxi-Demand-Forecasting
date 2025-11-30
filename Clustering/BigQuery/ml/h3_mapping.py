import pandas as pd
import h3pandas                     # để dùng .h3 accessor
import geopandas as gpd
from google.cloud import bigquery

from utils.bq_client import query_to_dataframe


def build_taxi_demand_centroid(
    client: bigquery.Client,
    zone_score_table: str = "nyc-taxi-project-479008.fact.zone_demand_score_pca",
    dim_h3_table: str = "nyc-taxi-project-479008.dimension.dim_h3_zone",
    ml_dataset_table: str = "nyc-taxi-project-479008.fact.ml_dataset",
    output_table: str = "nyc-taxi-project-479008.fact.taxi_demand_centroid",
) -> pd.DataFrame:
    """
    Map demand_score + trip_score theo zone_id xuống từng H3 cell,
    convert H3 → lat/lon centroid và ghi vào BigQuery.

    Output có schema:
      - h3_index (STRING)
      - zone_id (INT64)
      - pickup_zone_name (STRING)
      - demand_score (FLOAT64)
      - trip_score (FLOAT64)
      - lat (FLOAT64)
      - lon (FLOAT64)
    """

    # ======================================================
    # 1. Lấy demand_score + zone_name theo zone_id
    # ======================================================
    score_sql = f"""
    SELECT
      zone_id,
      pickup_zone_name,
      demand_score
    FROM `{zone_score_table}`
    """
    df_score = query_to_dataframe(score_sql, client)
    print("📥 Loaded zone scores:", df_score.shape[0], "rows")

    # ======================================================
    # 2. Tính trip_score từ fact.ml_dataset
    # ======================================================
    trip_sql = f"""
    SELECT
      zone_id,
      total_trips
    FROM `{ml_dataset_table}`
    """
    df_trips = query_to_dataframe(trip_sql, client)
    print("📥 Loaded total_trips:", df_trips.shape[0], "rows")

    # Chuẩn hóa total_trips → trip_score (0–100)
    trip_min = df_trips["total_trips"].min()
    trip_max = df_trips["total_trips"].max()

    df_trips["trip_score"] = 100 * (
        (df_trips["total_trips"] - trip_min) / (trip_max - trip_min)
    )

    print("📊 Trip score range:", df_trips["trip_score"].min(), "→", df_trips["trip_score"].max())

    # ======================================================
    # 3. Lấy mapping zone_id -> h3_index
    # ======================================================
    h3_sql = f"""
    SELECT
      zone_id,
      h3_index
    FROM `{dim_h3_table}`
    """
    df_h3 = query_to_dataframe(h3_sql, client)
    print("📥 Loaded H3 mapping:", df_h3.shape[0], "rows")

    # ======================================================
    # 4. Join tất cả vào: H3 + demand_score + trip_score
    # ======================================================
    df_join = (
        df_h3
        .merge(df_score, on="zone_id", how="inner")
        .merge(df_trips[["zone_id", "trip_score"]], on="zone_id", how="left")
    )

    print("🔗 Joined all data:", df_join.shape[0], "rows")

    # ======================================================
    # 5. Convert H3 → geometry centroid using h3pandas
    # ======================================================

    df_join["h3_index"] = df_join["h3_index"].astype(str)

    gdf = df_join.set_index("h3_index").h3.h3_to_geo()

    gdf = gdf.reset_index()

    gdf["lat"] = gdf.geometry.y
    gdf["lon"] = gdf.geometry.x

    # ======================================================
    # 6. Chọn các cột cần export
    # ======================================================
    result = gdf[
        [
            "h3_index",
            "zone_id",
            "pickup_zone_name",
            "demand_score",
            "trip_score",
            "lat",
            "lon",
        ]
    ].copy()

    print("\n📌 Preview centroid output:")
    print(result.head())

    # ======================================================
    # 7. Ghi vào BigQuery
    # ======================================================
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    job = client.load_table_from_dataframe(result, output_table, job_config=job_config)
    job.result()

    print(f"\n✅ Đã ghi bảng centroid vào: {output_table}")
    return result
