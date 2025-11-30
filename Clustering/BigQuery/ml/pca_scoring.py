# pca_scoring.py
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA


def compute_pca_demand_score(
    df: pd.DataFrame,
    output_table: str,
    client,
) -> pd.DataFrame:
    """
    Nhận dataframe đã sạch (không NaN), tính demand_score bằng PCA
    và ghi kết quả vào BigQuery.
    """

    # 4 thuộc tính dùng để học trọng số
    feature_cols = [
        "total_trips",
        "avg_hourly_demand",
        "trips_per_km2",
        "weekend_ratio",
    ]

    # Chỉ lấy các cột cần thiết, và dropna 1 lần nữa cho chắc
    df = df.dropna(subset=feature_cols).copy()

    X = df[feature_cols].values

    # Standardize
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # PCA 1 component
    pca = PCA(n_components=1)
    pca.fit(X_scaled)

    raw_weights = pca.components_[0]

    print("\n🔎 PCA Loadings (raw):")
    for f, w in zip(feature_cols, raw_weights):
        print(f"{f:17s} -> {w:.4f}")

    abs_weights = np.abs(raw_weights)
    normalized_weights = abs_weights / abs_weights.sum()

    print("\n📊 PCA Contribution (%):")
    for f, w in zip(feature_cols, normalized_weights):
        print(f"{f:17s} -> {w*100:.2f}%")

    # Điểm PC1
    pc1_scores = pca.transform(X_scaled)[:, 0]

    # Nếu nghịch dấu với total_trips thì đảo
    corr = np.corrcoef(pc1_scores, df["total_trips"])[0, 1]
    if corr < 0:
        pc1_scores = -pc1_scores

    # Chuẩn hoá 0–100
    pc1_min, pc1_max = pc1_scores.min(), pc1_scores.max()
    demand_score = 100 * (pc1_scores - pc1_min) / (pc1_max - pc1_min)
    df["demand_score"] = demand_score

    # Bảng kết quả
    result = (
        df[
            [
                "zone_id",
                "pickup_zone_name",
                "demand_score",
                "total_trips",
                "avg_hourly_demand",
                "trips_per_km2",
                "weekend_ratio",
            ]
        ]
        .sort_values("demand_score", ascending=False)
        .reset_index(drop=True)
    )
    result.insert(0, "rank", result.index + 1)

    print("\n🏆 Top 5 zone:")
    print(result.head())

    # Ghi BigQuery
    from google.cloud import bigquery
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(result, output_table, job_config=job_config)
    job.result()
    print(f"\n✅ Đã ghi kết quả PCA vào bảng: {output_table}")

    return result
