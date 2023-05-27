data "google_cloudfunctions_function" "fetch_ergast_files" {
  name = google_cloudfunctions_function.fetch_ergast_files.name
}

resource "google_cloudfunctions_function" "fetch_ergast_files" {
  name    = "cf-fetch_ergast_files-${var.environment}"
  runtime = "python310"

  available_memory_mb          = 256
  timeout                      = 60
  trigger_http                 = true
  source_archive_bucket        = data.google_storage_bucket.default_bucket.name
  source_archive_object        = google_storage_bucket_object.fetch_ergast_files.name
  https_trigger_security_level = "SECURE_ALWAYS"

  entry_point = "main"
}
