"""Quick check for hourly_demand_forecast data"""
from google.cloud import bigquery
import os

# Use default credentials (gcloud auth application-default login)
client = bigquery.Client(project='nyc-taxi-project-477115')

# Check hourly_demand_forecast
query = """
SELECT 
    COUNT(*) as row_count,
    COUNT(DISTINCT pickup_h3_id) as unique_zones,
    MIN(timestamp_hour) as earliest_forecast,
    MAX(timestamp_hour) as latest_forecast
FROM `nyc-taxi-project-477115.ml_predictions.hourly_demand_forecast`
"""

print("🔍 Checking hourly_demand_forecast table...")
try:
    result = client.query(query).to_dataframe()
    print(result)
    
    if result['row_count'].iloc[0] == 0:
        print("\n❌ Table is EMPTY! Need to run ML pipeline to generate forecasts.")
        print("💡 Run: gcloud workflows execute daily-ml-pipeline --location=us-central1")
    else:
        print(f"\n✅ Table has data: {result['row_count'].iloc[0]} rows")
        
except Exception as e:
    print(f"❌ Error: {e}")
    print("\n💡 Make sure you're authenticated:")
    print("   gcloud auth application-default login")
