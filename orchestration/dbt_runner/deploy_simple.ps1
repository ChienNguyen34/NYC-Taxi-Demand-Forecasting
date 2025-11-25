# Simple dbt automation via Cloud Scheduler calling BigQuery directly
# No Cloud Build, no Cloud Function - just periodic view refresh

$PROJECT_ID = "nyc-taxi-project-477115"
$REGION = "us-central1"

Write-Host "`n=== Setting up simple dbt automation ===" -ForegroundColor Cyan

# Delete old scheduler (it has wrong config)
Write-Host "`nDeleting old scheduler job..." -ForegroundColor Yellow
gcloud scheduler jobs delete dbt-pipeline-daily --location=$REGION --project=$PROJECT_ID --quiet 2>$null

# Create new scheduler that triggers view refresh via HTTP endpoint
# We'll create a simple Cloud Function that runs bq queries to refresh views

Write-Host "`nDeploying dbt refresh function..." -ForegroundColor Yellow
Set-Location "$PSScriptRoot\..\dbt_runner"

gcloud functions deploy dbt-refresh `
    --gen2 `
    --runtime=python311 `
    --region=$REGION `
    --source=. `
    --entry-point=run_dbt `
    --trigger-http `
    --allow-unauthenticated `
    --timeout=540s `
    --memory=512MB `
    --project=$PROJECT_ID

# Get function URL
$FUNCTION_URL = (gcloud functions describe dbt-refresh --gen2 --region=$REGION --project=$PROJECT_ID --format="value(serviceConfig.uri)")

Write-Host "`nFunction URL: $FUNCTION_URL" -ForegroundColor Green

# Create scheduler to call function every hour
Write-Host "`nCreating hourly scheduler..." -ForegroundColor Yellow
gcloud scheduler jobs create http dbt-pipeline-hourly `
    --location=$REGION `
    --schedule="0 * * * *" `
    --time-zone="America/New_York" `
    --uri=$FUNCTION_URL `
    --http-method=GET `
    --project=$PROJECT_ID

Write-Host "`n✅ Simple dbt automation complete!" -ForegroundColor Green
Write-Host "`nSchedule: Every hour at :00" -ForegroundColor Cyan
Write-Host "Function URL: $FUNCTION_URL" -ForegroundColor White

Write-Host "`nTo trigger manually:" -ForegroundColor Cyan
Write-Host "  curl $FUNCTION_URL" -ForegroundColor White
