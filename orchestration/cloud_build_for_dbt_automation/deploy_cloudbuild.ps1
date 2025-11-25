# deploy_cloudbuild.ps1
# Deploy Cloud Build trigger + Cloud Scheduler for daily dbt runs

$PROJECT_ID = "nyc-taxi-project-477115"
$REGION = "us-central1"
$REPO_OWNER = "ChienNguyen34"
$REPO_NAME = "NYC-Taxi-Demand-Forecasting"
$BRANCH = "main"

Write-Host "`n=== Setting up Cloud Build for dbt ===" -ForegroundColor Cyan

# Step 1: Enable Cloud Build API
Write-Host "`nEnabling Cloud Build API..." -ForegroundColor Yellow
gcloud services enable cloudbuild.googleapis.com --project=$PROJECT_ID

# Step 2: Grant Cloud Build service account BigQuery permissions
Write-Host "`nGranting BigQuery permissions to Cloud Build..." -ForegroundColor Yellow
$PROJECT_NUMBER = (gcloud projects describe $PROJECT_ID --format="value(projectNumber)")
$CLOUDBUILD_SA = "${PROJECT_NUMBER}@cloudbuild.gserviceaccount.com"

gcloud projects add-iam-policy-binding $PROJECT_ID `
    --member="serviceAccount:$CLOUDBUILD_SA" `
    --role="roles/bigquery.dataEditor" `
    --condition=None

gcloud projects add-iam-policy-binding $PROJECT_ID `
    --member="serviceAccount:$CLOUDBUILD_SA" `
    --role="roles/bigquery.jobUser" `
    --condition=None

# Step 3: Create Cloud Build trigger (manual + webhook)
Write-Host "`nCreating Cloud Build trigger..." -ForegroundColor Yellow
gcloud builds triggers create github `
    --name="dbt-daily-run" `
    --repo-name=$REPO_NAME `
    --repo-owner=$REPO_OWNER `
    --branch-pattern="^main$" `
    --build-config="cloudbuild.yaml" `
    --region=$REGION `
    --project=$PROJECT_ID `
    2>$null

if ($LASTEXITCODE -ne 0) {
    Write-Host "Trigger already exists, skipping creation..." -ForegroundColor Yellow
}

# Step 4: Get trigger webhook URL
Write-Host "`nGetting trigger webhook..." -ForegroundColor Yellow
$TRIGGER_ID = (gcloud builds triggers describe dbt-daily-run --region=$REGION --project=$PROJECT_ID --format="value(id)")
$WEBHOOK_URL = "https://cloudbuild.googleapis.com/v1/projects/$PROJECT_ID/locations/$REGION/triggers/${TRIGGER_ID}:webhook?key=AIzaSyDummy&secret=dummy"

# Step 5: Create Cloud Scheduler job to trigger Cloud Build
Write-Host "`nCreating Cloud Scheduler job..." -ForegroundColor Yellow
gcloud scheduler jobs create http dbt-pipeline-daily `
    --location=$REGION `
    --schedule="30 9 * * *" `
    --time-zone="America/New_York" `
    --uri="https://cloudbuild.googleapis.com/v1/projects/$PROJECT_ID/locations/$REGION/builds" `
    --message-body="{
        'source': {
            'repoSource': {
                'projectId': '$PROJECT_ID',
                'repoName': 'github_${REPO_OWNER}_${REPO_NAME}',
                'branchName': '$BRANCH'
            }
        },
        'steps': [
            {
                'name': 'gcr.io/cloud-builders/gcloud',
                'args': ['builds', 'submit', '--config=cloudbuild.yaml', '--project=$PROJECT_ID']
            }
        ]
    }" `
    --http-method=POST `
    --oauth-service-account-email="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" `
    --project=$PROJECT_ID `
    2>$null

if ($LASTEXITCODE -ne 0) {
    Write-Host "Scheduler job already exists" -ForegroundColor Yellow
}

Write-Host "`n✅ Cloud Build setup complete!" -ForegroundColor Green
Write-Host "`nSchedule: Daily at 4:30 AM EST" -ForegroundColor Cyan
Write-Host "  - 4:30 AM: dbt run (refresh all models)" -ForegroundColor White
Write-Host "  - 6:00 AM: ML pipeline (train + predict)" -ForegroundColor White

Write-Host "`nTo trigger dbt manually:" -ForegroundColor Cyan
Write-Host "  gcloud builds submit --config=cloudbuild.yaml --region=$REGION --project=$PROJECT_ID" -ForegroundColor White

Write-Host "`nTo view build history:" -ForegroundColor Cyan
Write-Host "  https://console.cloud.google.com/cloud-build/builds?project=$PROJECT_ID" -ForegroundColor White
