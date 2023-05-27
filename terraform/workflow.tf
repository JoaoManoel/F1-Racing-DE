data "google_app_engine_default_service_account" "default" {
}

resource "google_workflows_workflow" "update-F1Racing" {
  name            = "wfs-update-F1Racing-${var.environment}"
  region          = var.region
  service_account = data.google_app_engine_default_service_account.default.email
  source_contents = <<-EOF
  main:
      steps:
      - fetchErgastFiles:
          call: http.post
          args:
              url: ${data.google_cloudfunctions_function.fetch_ergast_files.https_trigger_url}
              headers:
                  "Content-Type": "application/json"
              body:
                  "bucket_name": ${var.bucket-landing-zone}
                  "folder_name": "F1_Racing"
              auth:
                  type: OIDC
          result: 'ok'

      - startProcessing:
          call: http.post
          args:
              url: https://dataproc.googleapis.com/v1/projects/${var.project}/locations/us-central1/batches/
              headers:
                  "Content-Type": "application/json"
              body:
                  pysparkBatch:
                      args:
                      - gs://${var.bucket-landing-zone}/F1_Racing
                      - gs://${var.bucket-curated-zone}/F1_Racing
                      mainPythonFileUri: gs://${data.google_storage_bucket.default_bucket.name}/pyspark_scripts/processing.py
                  runtimeConfig:
                      version: '2.1'
                  environmentConfig:
                      executionConfig:
                          subnetworkUri: default
              auth:
                  type: OAuth2
          result: job

      - getStartProcessingStatus:
          call: http.get
          args:
              url: $${"https://dataproc.googleapis.com/v1/" + job.body.metadata.batch}
              auth:
                  type: OAuth2
          result: jobStatus

      - checkStartProcessingJob:
          switch:
              - condition: $${jobStatus.body.state != "PENDING" and jobStatus.body.state != "RUNNING"}
                next: startAggregating

      - wait1:
          call: sys.sleep
          args:
              seconds: 60
          next: getStartProcessingStatus


      - startAggregating:
          call: http.post
          args:
              url: https://dataproc.googleapis.com/v1/projects/${var.project}/locations/us-central1/batches/
              headers:
                  "Content-Type": "application/json"
              body:
                  pysparkBatch:
                      args:
                      - gs://${var.bucket-curated-zone}/F1_Racing
                      - gs://${var.bucket-presentation-zone}/F1_Racing
                      mainPythonFileUri: gs://${data.google_storage_bucket.default_bucket.name}/pyspark_scripts/aggregating.py
                  runtimeConfig:
                      version: '2.1'
                  environmentConfig:
                      executionConfig:
                          subnetworkUri: default
              auth:
                  type: OAuth2
          result: job

      - getStartAggregatingStatus:
          call: http.get
          args:
              url: $${"https://dataproc.googleapis.com/v1/" + job.body.metadata.batch}
              auth:
                  type: OAuth2
          result: jobStatus

      - checkStartAggregatingJob:
          switch:
              - condition: $${jobStatus.body.state != "PENDING" and jobStatus.body.state != "RUNNING"}
                next: updateBQ

      - wait2:
          call: sys.sleep
          args:
              seconds: 60
          next: getStartAggregatingStatus

      - updateBQ:
          call: http.post
          args:
              url: https://dataproc.googleapis.com/v1/projects/${var.project}/locations/us-central1/batches/
              headers:
                  "Content-Type": "application/json"
              body:
                  pysparkBatch:
                      args:
                      - gs://${var.bucket-presentation-zone}/F1_Racing
                      - ${var.project}.F1_Racing2
                      jarFileUris:
                      - gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.30.0.jar
                      - gs://hadoop-lib/gcs/gcs-connector-hadoop3-2.2.13.jar
                      mainPythonFileUri: gs://${data.google_storage_bucket.default_bucket.name}/pyspark_scripts/update_bq.py
                  runtimeConfig:
                      version: '2.1'
                  environmentConfig:
                      executionConfig:
                          subnetworkUri: default
              auth:
                  type: OAuth2
          result: 'ok'
  EOF
}
