import os
import json
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import contextily as cx
from shapely.geometry import Point


def visual_map(
    json_path: str = "data/taxi_demand_centroid.json",
    output_path: str = "output/taxi_demand_map.png"
):
    """
    Vẽ map gồm:
        - Trip Score map
        - PCA Demand Score map
    Vừa HIỂN THỊ map, vừa LƯU ảnh PNG.
    """

    print(f"📂 Working directory: {os.getcwd()}")

    # ================================
    # 1. Load JSON
    # ================================
    if not os.path.exists(json_path):
        raise FileNotFoundError(f"Không tìm thấy file JSON: {json_path}")

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    df = pd.DataFrame(data)
    print("Loaded rows:", df.shape[0])

    # ================================
    # 2. GeoDataFrame
    # ================================
    df["geometry"] = df.apply(lambda r: Point(r["lon"], r["lat"]), axis=1)
    gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")

    gdf_web = gdf.to_crs(epsg=3857)

    # ================================
    # 3. Tạo figure
    # ================================
    fig, axes = plt.subplots(1, 2, figsize=(18, 9))

    # ----------------------------
    # MAP 1 – Trip score
    # ----------------------------
    ax1 = axes[0]

    if "trip_score" not in gdf_web.columns:
        print("⚠ trip_score không có → fallback demand_score")
        gdf_web["trip_score"] = gdf_web["demand_score"]

    sizes_trip = 4 + (gdf_web["trip_score"] / gdf_web["trip_score"].max()) * 85

    gdf_web.plot(
        ax=ax1,
        column="trip_score",
        cmap="YlGnBu",
        s=sizes_trip,
        alpha=1.0,
        legend=True
    )

    try:
        cx.add_basemap(
            ax1,
            source=cx.providers.CartoDB.Positron,
            zoom=11,
            alpha=0.4
        )
    except Exception as e:
        print("⚠ Không tải basemap MAP 1:", e)

    ax1.set_axis_off()
    ax1.set_title("Demand by Trips Only (trip_score)", fontsize=14)

    # ----------------------------
    # MAP 2 – PCA score
    # ----------------------------
    ax2 = axes[1]

    sizes_pca = 4 + (gdf_web["demand_score"] / gdf_web["demand_score"].max()) * 85

    gdf_web.plot(
        ax=ax2,
        column="demand_score",
        cmap="YlGnBu",
        s=sizes_pca,
        alpha=1.0,
        legend=True
    )

    try:
        cx.add_basemap(
            ax2,
            source=cx.providers.CartoDB.Positron,
            zoom=11,
            alpha=0.4
        )
    except Exception as e:
        print("⚠ Không tải basemap MAP 2:", e)

    ax2.set_axis_off()
    ax2.set_title("Demand (PCA from multiple factors)", fontsize=14)

    # ----------------------------
    plt.tight_layout()

    # ================================
    # 4. SAVE PNG
    # ================================
    out_dir = os.path.dirname(output_path)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    abs_path = os.path.abspath(output_path)
    plt.savefig(abs_path, dpi=300, bbox_inches="tight")
    print(f"📸 Saved PNG to: {abs_path}")

    # ================================
    # 5. SHOW MAP (cửa sổ bật lên)
    # ================================
    plt.show()

    return abs_path
