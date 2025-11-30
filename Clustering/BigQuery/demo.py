from utils.bq_client import get_bq_client, load_sql_file
from ml.preprocessing import preprocess_ml_dataset
from ml.pca_scoring import compute_pca_demand_score
from ml.h3_mapping import build_taxi_demand_centroid
from ml.export_json import export_centroid_to_json
from ml.visual_map import visual_map



def execute_sql(client, filename: str):
    sql = load_sql_file(filename)
    job = client.query(sql)
    job.result()
    print(f"✔️ Executed: {filename}")


def main():
    client = get_bq_client()

    # Step 1: RAW 
    execute_sql(client, "tlc_yellow_trips_2021_raw.sql")

    # Step 2: STAGING
    execute_sql(client, "staging_raw_data.sql")

    # Step 3: DIMENSIONS
    execute_sql(client, "dim_datetime.sql")
    execute_sql(client, "dim_h3.sql")
    execute_sql(client, "dim_zone.sql")
    execute_sql(client, "dim_weather.sql")

    # Step 4: FACT 
    execute_sql(client, "fact_trip.sql")
    execute_sql(client, "fact_demand_zone_hour.sql")
    execute_sql(client, "fact_demand_zone_hour_weather.sql")

    # Step 5: MACHINE LEARNING MODEL
    execute_sql(client, "ml_data.sql")              # prepare data
    df_clean = preprocess_ml_dataset(client)        # load, preprocessing (Clean, null removing)
    compute_pca_demand_score(
        df=df_clean,
        output_table="nyc-taxi-project-479008.fact.zone_demand_score_pca",
        client=client,
    )
    build_taxi_demand_centroid(client)

    # Step 6: EXPORT JSON FOR VISULIZATION
    export_centroid_to_json(client)

    visual_map(
        json_path="data/taxi_demand_centroid.json",
        output_path="output/taxi_demand_map.png",
    )

if __name__ == "__main__":
    main()
