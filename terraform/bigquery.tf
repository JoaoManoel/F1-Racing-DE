resource "random_string" "random" {
  length  = 6
  special = false
}

resource "google_bigquery_dataset" "dataset" {
  dataset_id = "F1_Racing"
  location   = "us-central1"

  labels = {
    project = "f1_racing"
  }
}

resource "google_bigquery_table" "races" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = "races"

  labels = {
    project = "f1_racing"
  }

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris = [
      "gs://${var.bucket-curated-zone}/F1_Racing/races/*.parquet"
    ]

    hive_partitioning_options {
      mode                     = "AUTO"
      source_uri_prefix        = "gs://${var.bucket-curated-zone}/F1_Racing/races/"
      require_partition_filter = true
    }
  }
}

resource "google_bigquery_table" "races_results" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = "races_results"

  labels = {
    project = "f1_racing"
  }

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris = [
      "gs://${var.bucket-presentation-zone}/F1_Racing/race_results/part-*.parquet"
    ]
  }
}

resource "google_bigquery_table" "constructor_standings" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = "constructor_standings"

  labels = {
    project = "f1_racing"
  }
}

resource "google_bigquery_table" "driver_standings" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = "driver_standings"

  labels = {
    project = "f1_racing"
  }
}

resource "google_bigquery_job" "constructor_standings_job" {
  job_id   = "job_load_${random_string.random.result}c"
  location = "us-central1"

  load {
    source_uris = [
      "gs://${var.bucket-presentation-zone}/F1_Racing/constructor_standings/part-*.parquet"
    ]

    destination_table {
      project_id = google_bigquery_table.constructor_standings.project
      dataset_id = google_bigquery_table.constructor_standings.dataset_id
      table_id   = google_bigquery_table.constructor_standings.table_id
    }

    write_disposition = "WRITE_TRUNCATE"
    source_format     = "PARQUET"
  }
}

resource "google_bigquery_job" "driver_standings_job" {
  job_id   = "job_load_${random_string.random.result}d"
  location = "us-central1"

  load {
    source_uris = [
      "gs://${var.bucket-presentation-zone}/F1_Racing/driver_standings/part-*.parquet"
    ]

    destination_table {
      project_id = google_bigquery_table.driver_standings.project
      dataset_id = google_bigquery_table.driver_standings.dataset_id
      table_id   = google_bigquery_table.driver_standings.table_id
    }

    write_disposition = "WRITE_TRUNCATE"
    source_format     = "PARQUET"
  }
}
