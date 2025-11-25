# Cloud Function to refresh BigQuery views (dbt models)
import functions_framework
from google.cloud import bigquery

@functions_framework.http
def run_dbt(request):
    """HTTP Cloud Function to refresh dbt view models"""
    
    try:
        client = bigquery.Client(project="nyc-taxi-project-477115")
        
        # Simply query the views to trigger refresh
        # BigQuery will re-execute the view definitions
        views_to_refresh = [
            "staging_layer.stg_taxi_trips",
            "facts.fct_trips", 
            "facts.agg_hourly_demand_h3",
            "facts.fct_hourly_features"
        ]
        
        results = []
        for view in views_to_refresh:
            query = f"SELECT COUNT(*) as cnt FROM `nyc-taxi-project-477115.{view}`"
            query_job = client.query(query)
            result = list(query_job.result())[0]
            results.append(f"{view}: {result.cnt} rows")
        
        return {
            "status": "success",
            "message": "Views refreshed successfully",
            "results": results
        }, 200
        
    except Exception as e:
        return {
            "status": "error", 
            "message": str(e)
        }, 500
