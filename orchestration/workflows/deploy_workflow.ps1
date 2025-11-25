# workflows/deploy_workflow.ps1
# Deploy Cloud Workflow and setup daily scheduler

$PROJECT_ID = "nyc-taxi-project-477115"
$REGION = "us-central1"
$WORKFLOW_NAME = "daily-ml-pipeline"

Write-Host "Deploying Cloud Workflow..." -ForegroundColor Cyan

# Deploy workflow
gcloud workflows deploy $WORKFLOW_NAME `
  --source=daily_pipeline.yaml `
  --location=$REGION `
  --project=$PROJECT_ID `
  --service-account=868896275886-compute@developer.gserviceaccount.com

Write-Host "`n✅ Workflow deployed successfully!" -ForegroundColor Green

# Create Cloud Scheduler job to trigger workflow daily at 6:00 AM
Write-Host "`nCreating Cloud Scheduler job..." -ForegroundColor Cyan

gcloud scheduler jobs create http ml-pipeline-daily `
  --location=$REGION `
  --schedule="0 6 * * *" `
  --uri="https://workflowexecutions.googleapis.com/v1/projects/$PROJECT_ID/locations/$REGION/workflows/$WORKFLOW_NAME/executions" `
  --http-method=POST `
  --oauth-service-account-email=868896275886-compute@developer.gserviceaccount.com `
  --project=$PROJECT_ID `
  --time-zone="America/New_York"

Write-Host "`n✅ Scheduler created!" -ForegroundColor Green
Write-Host "`nSchedule: Daily at 6:00 AM EST" -ForegroundColor Cyan
Write-Host "`nTo trigger manually:" -ForegroundColor Yellow
Write-Host "  gcloud workflows execute $WORKFLOW_NAME --location=$REGION --project=$PROJECT_ID"
Write-Host "`nTo view executions:" -ForegroundColor Yellow
Write-Host "  https://console.cloud.google.com/workflows/workflow/$REGION/$WORKFLOW_NAME/executions?project=$PROJECT_ID"
