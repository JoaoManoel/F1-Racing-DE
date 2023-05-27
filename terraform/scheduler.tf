resource "google_cloud_scheduler_job" "job" {
  name      = "sch-update-F1Racing-${var.environment}"
  schedule  = "0 1 * * 1"
  time_zone = "Etc/GMT"

  http_target {
    http_method = "POST"
    uri         = "https://workflowexecutions.googleapis.com/v1/projects/${var.project}/locations/${var.region}/workflows/${google_workflows_workflow.update-F1Racing.name}/executions"
    body        = base64encode("{\"argument\":\"{}\",\"callLogLevel\":\"LOG_ERRORS_ONLY\"}")

    oauth_token {
      service_account_email = data.google_app_engine_default_service_account.default.email
    }
  }
}
