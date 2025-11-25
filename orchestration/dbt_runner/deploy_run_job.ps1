# Simplest approach: Cloud Run Job runs dbt directly
# No function, just containerized dbt execution

$PROJECT_ID = "nyc-taxi-project-477115"
$REGION = "us-central1"
$JOB_NAME = "dbt-hourly-refresh"

Write-Host "`n=== Creating Cloud Run Job for dbt ===" -ForegroundColor Cyan

# Build and deploy a Cloud Run Job that runs dbt
Write-Host "`nCreating Dockerfile..." -ForegroundColor Yellow

# Create temporary directory for build
$BUILD_DIR = "$PSScriptRoot\temp_build"
New-Item -ItemType Directory -Force -Path $BUILD_DIR | Out-Null

# Copy necessary files
Copy-Item -Path "$PSScriptRoot\..\..\nyc_taxi_pipeline\*" -Destination $BUILD_DIR -Recurse -Force

# Create Dockerfile
@"
FROM python:3.11-slim

WORKDIR /dbt

# Install dbt
RUN pip install --no-cache-dir dbt-core==1.8.7 dbt-bigquery==1.8.2

# Copy dbt project
COPY . .

# Run dbt when container starts
CMD ["dbt", "run", "--profiles-dir", ".", "--full-refresh"]
"@ | Out-File -FilePath "$BUILD_DIR\Dockerfile" -Encoding UTF8

Write-Host "`nBuilding and deploying Cloud Run Job..." -ForegroundColor Yellow
Set-Location $BUILD_DIR

gcloud run jobs create $JOB_NAME `
    --source=. `
    --region=$REGION `
    --project=$PROJECT_ID `
    --memory=2Gi `
    --cpu=2 `
    --max-retries=2 `
    --task-timeout=10m `
    --execute-now=false

# Get job URL
$JOB_URL = "https://$REGION-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/$PROJECT_ID/jobs/${JOB_NAME}:run"

Write-Host "`nDeleting old scheduler..." -ForegroundColor Yellow
gcloud scheduler jobs delete dbt-pipeline-daily --location=$REGION --project=$PROJECT_ID --quiet 2>$null
gcloud scheduler jobs delete dbt-pipeline-hourly --location=$REGION --project=$PROJECT_ID --quiet 2>$null

Write-Host "`nCreating new hourly scheduler..." -ForegroundColor Yellow
gcloud scheduler jobs create http dbt-refresh-hourly `
    --location=$REGION `
    --schedule="0 * * * *" `
    --time-zone="America/New_York" `
    --uri=$JOB_URL `
    --http-method=POST `
    --oauth-service-account-email="868896275886-compute@developer.gserviceaccount.com" `
    --project=$PROJECT_ID

# Clean up
Set-Location $PSScriptRoot
Remove-Item -Path $BUILD_DIR -Recurse -Force

Write-Host "`n✅ Cloud Run Job setup complete!" -ForegroundColor Green
Write-Host "`nSchedule: Every hour" -ForegroundColor Cyan
Write-Host "`nTo trigger manually:" -ForegroundColor Cyan
Write-Host "  gcloud run jobs execute $JOB_NAME --region=$REGION --project=$PROJECT_ID" -ForegroundColor White
