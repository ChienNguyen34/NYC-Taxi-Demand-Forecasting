# streaming/deploy_functions.ps1
# Deploy all 4 Cloud Functions

$PROJECT_ID = "nyc-taxi-project-477115"
$REGION = "us-central1"
$OPENWEATHER_API_KEY = $env:OPENWEATHER_API_KEY  # Đọc từ environment variable

if (-not $OPENWEATHER_API_KEY) {
    Write-Host "ERROR: OPENWEATHER_API_KEY not set! Run: `$env:OPENWEATHER_API_KEY='your_key'" -ForegroundColor Red
    exit 1
}

Write-Host "Deploying Cloud Functions..." -ForegroundColor Cyan

# Function 1: Fetch Weather and Publish to Pub/Sub (HTTP trigger)
Write-Host "`n[1/4] Deploying fetch_weather_and_publish..." -ForegroundColor Yellow
gcloud functions deploy fetch-weather `
  --gen2 `
  --runtime=python311 `
  --region=$REGION `
  --source=. `
  --entry-point=fetch_weather_and_publish `
  --trigger-http `
  --allow-unauthenticated `
  --env-vars-file=.env.yaml `
  --set-secrets="OPENWEATHER_API_KEY=OPENWEATHER_API_KEY:latest" `
  --project=$PROJECT_ID

# Function 2: Insert Weather Data to BigQuery (Pub/Sub trigger)
Write-Host "`n[2/4] Deploying insert_weather_data_to_bq..." -ForegroundColor Yellow
gcloud functions deploy insert-weather `
  --gen2 `
  --runtime=python311 `
  --region=$REGION `
  --source=. `
  --entry-point=insert_weather_data_to_bq `
  --trigger-topic=weather-stream `
  --env-vars-file=.env.yaml `
  --project=$PROJECT_ID

# Function 3: Fetch Taxi Trips and Publish to Pub/Sub (HTTP trigger)
Write-Host "`n[3/4] Deploying fetch_taxi_trips_and_publish..." -ForegroundColor Yellow
gcloud functions deploy fetch-taxi-trips `
  --gen2 `
  --runtime=python311 `
  --region=$REGION `
  --source=. `
  --entry-point=fetch_taxi_trips_and_publish `
  --trigger-http `
  --allow-unauthenticated `
  --env-vars-file=.env.yaml `
  --timeout=540s `
  --memory=512MB `
  --project=$PROJECT_ID

# Function 4: Insert Taxi Trips to BigQuery (Pub/Sub trigger)
Write-Host "`n[4/4] Deploying insert_taxi_trips_to_bq..." -ForegroundColor Yellow
gcloud functions deploy insert-taxi-trips `
  --gen2 `
  --runtime=python311 `
  --region=$REGION `
  --source=. `
  --entry-point=insert_taxi_trips_to_bq `
  --trigger-topic=taxi-stream `
  --env-vars-file=.env.yaml `
  --project=$PROJECT_ID

Write-Host "`n✅ All functions deployed successfully!" -ForegroundColor Green
Write-Host "`nNext steps:" -ForegroundColor Cyan
Write-Host "1. Setup Cloud Scheduler to trigger functions periodically"
Write-Host "2. Run: .\setup_scheduler.ps1"
