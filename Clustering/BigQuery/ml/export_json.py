import os
import json
from google.cloud import bigquery
from utils.bq_client import query_to_dataframe


def export_centroid_to_json(
    client: bigquery.Client,
    centroid_table: str = "nyc-taxi-project-479008.fact.taxi_demand_centroid",
    output_path: str = "data/taxi_demand_centroid.json"
):
    """
    Xuất bảng taxi_demand_centroid từ BigQuery ra file JSON để dùng vẽ map (HTML/JS).

    Output JSON gồm:
      - h3_index
      - zone_id
      - pickup_zone_name
      - demand_score
      - trip_score   <-- đã thêm
      - lat
      - lon
    """

    print(f"📥 Querying centroid table: {centroid_table}")

    query = f"""
    SELECT
      h3_index,
      zone_id,
      pickup_zone_name,
      demand_score,
      trip_score,          -- NEW
      lat,
      lon
    FROM `{centroid_table}`
    """

    df = query_to_dataframe(query, client)
    print("🔢 Rows loaded:", df.shape[0])

    # Tạo thư mục nếu chưa có
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Chuyển DataFrame → danh sách dict để JSON đọc được
    records = df.to_dict(orient="records")

    # Ghi JSON
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False)

    print(f"✅ Đã ghi file JSON: {output_path}")
    return output_path
