# ---------------------------------------------------------------------------
# modules/glue/main.tf
#
# Provisions the AWS Glue catalog database and the three ETL jobs:
#   - validation_job        (Python Shell)  : schema validation of raw CSVs
#   - transformation_job    (Spark / glueetl): KPI transformation logic
#   - dynamodb_ingestion_job(Python Shell)  : writes KPI results to DynamoDB
#
# Job scripts are expected at s3://<reference_bucket>/scripts/<job>.py.
# The reference bucket name and the Glue execution role ARN are passed in.
# ---------------------------------------------------------------------------

locals {
  scripts_base = "s3://${var.reference_bucket_name}/scripts"
}

# Glue Data Catalog database.
resource "aws_glue_catalog_database" "database" {
  name = var.database_name
}

# Job 1 — schema validation (Python Shell).
resource "aws_glue_job" "validation" {
  name         = var.validation_job_name
  role_arn     = var.glue_role_arn
  max_capacity = 0.0625
  max_retries  = 0

  command {
    name            = "pythonshell"
    python_version  = "3.9"
    script_location = "${local.scripts_base}/validation_job.py"
  }

  default_arguments = {
    "--enable-continuous-cloudwatch-log" = "true"
    "--raw_bucket"                       = var.raw_bucket_name
  }

  tags = var.tags
}

# Job 2 — transformation (Spark / Glue ETL).
resource "aws_glue_job" "transformation" {
  name              = var.transformation_job_name
  role_arn          = var.glue_role_arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "${local.scripts_base}/transformation_job.py"
  }

  default_arguments = {
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${var.reference_bucket_name}/spark-logs/"
    "--job-language"                     = "python"
    "--raw_bucket"                       = var.raw_bucket_name
    "--reference_bucket"                 = var.reference_bucket_name
    "--processed_bucket"                 = var.processed_bucket_name
    "--quarantine_bucket"                = var.quarantine_bucket_name
  }

  tags = var.tags
}

# Job 3 — DynamoDB ingestion (Python Shell).
resource "aws_glue_job" "dynamodb_ingestion" {
  name         = var.dynamodb_ingestion_job_name
  role_arn     = var.glue_role_arn
  max_capacity = 0.0625
  max_retries  = 0

  command {
    name            = "pythonshell"
    python_version  = "3.9"
    script_location = "${local.scripts_base}/dynamodb_ingestion_job.py"
  }

  default_arguments = {
    "--enable-continuous-cloudwatch-log" = "true"
    "--processed_bucket"                 = var.processed_bucket_name
    "--dynamodb_table"                   = var.dynamodb_table_name
  }

  tags = var.tags
}
