data "google_storage_bucket" "default_bucket" {
  name = var.default-bucket != "" ? var.default-bucket : var.project
}

data "archive_file" "fetch_ergast_files" {
  type        = "zip"
  source_dir  = "../src/cloudfunctions/fetch_ergast_files"
  output_path = "./tmp/function.zip"
}

resource "google_storage_bucket_object" "fetch_ergast_files" {
  name         = "cloudfunctions/fetch_ergast_files-${data.archive_file.fetch_ergast_files.output_md5}.zip"
  content_type = "application/zip"
  source       = data.archive_file.fetch_ergast_files.output_path
  bucket       = data.google_storage_bucket.default_bucket.name
}

resource "google_storage_bucket_object" "pyspark_scripts" {
  for_each = fileset("${path.module}/../src/pyspark_scripts", "*.py")

  name         = "pyspark_scripts/${each.value}"
  content_type = "application/x-python-code"
  source       = "${path.module}/../src/pyspark_scripts/${each.value}"
  bucket       = data.google_storage_bucket.default_bucket.name
}
