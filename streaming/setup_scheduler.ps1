# streaming/setup_scheduler.ps1
# Tạo Cloud Scheduler jobs để trigger functions định kỳ

$PROJECT_ID = "nyc-taxi-project-477115"
$REGION = "us-central1"

Write-Host "Creating Cloud Scheduler jobs..." -ForegroundColor Cyan

# Get function URLs
$WEATHER_URL = gcloud functions describe fetch-weather --gen2 --region=$REGION --project=$PROJECT_ID --format="value(serviceConfig.uri)"
$TAXI_URL = gcloud functions describe fetch-taxi-trips --gen2 --region=$REGION --project=$PROJECT_ID --format="value(serviceConfig.uri)"

Write-Host "`nWeather Function URL: $WEATHER_URL" -ForegroundColor Gray
Write-Host "Taxi Function URL: $TAXI_URL" -ForegroundColor Gray

# Job 1: Fetch weather every 15 minutes
Write-Host "`n[1/2] Creating weather-fetcher job (every 15 min)..." -ForegroundColor Yellow
gcloud scheduler jobs create http weather-fetcher `
  --location=$REGION `
  --schedule="*/15 * * * *" `
  --uri=$WEATHER_URL `
  --http-method=GET `
  --project=$PROJECT_ID `
  --attempt-deadline=60s

# Job 2: Fetch taxi trips every 5 minutes
Write-Host "`n[2/2] Creating taxi-simulator job (every 5 min)..." -ForegroundColor Yellow
gcloud scheduler jobs create http taxi-simulator `
  --location=$REGION `
  --schedule="*/5 * * * *" `
  --uri=$TAXI_URL `
  --http-method=POST `
  --message-body='{\"date\":\"2025-11-24\"}' `
  --headers="Content-Type=application/json" `
  --project=$PROJECT_ID `
  --attempt-deadline=540s

Write-Host "`n✅ Cloud Scheduler jobs created!" -ForegroundColor Green
Write-Host "`nSchedules:" -ForegroundColor Cyan
Write-Host "  - Weather: Every 15 minutes"
Write-Host "  - Taxi: Every 5 minutes (50 trips/batch = 600 trips/hour)"
Write-Host "`nTo start jobs manually:" -ForegroundColor Cyan
Write-Host "  gcloud scheduler jobs run weather-fetcher --location=$REGION --project=$PROJECT_ID"
Write-Host "  gcloud scheduler jobs run taxi-simulator --location=$REGION --project=$PROJECT_ID"
