# streaming/setup_pubsub.ps1
# Tạo Pub/Sub topics và subscriptions (PowerShell version)

$PROJECT_ID = "nyc-taxi-project-477115"

# Create topics
gcloud pubsub topics create weather-stream --project=$PROJECT_ID
gcloud pubsub topics create taxi-stream --project=$PROJECT_ID

# Create subscriptions
gcloud pubsub subscriptions create weather-stream-sub `
  --topic=weather-stream `
  --project=$PROJECT_ID

gcloud pubsub subscriptions create taxi-stream-sub `
  --topic=taxi-stream `
  --project=$PROJECT_ID

Write-Host "Pub/Sub topics created successfully!" -ForegroundColor Green
