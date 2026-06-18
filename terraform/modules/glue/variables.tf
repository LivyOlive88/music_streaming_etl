# ---------------------------------------------------------------------------
# modules/glue/variables.tf
# Input variables for the Glue module.
# ---------------------------------------------------------------------------

variable "database_name" {
  description = "Name of the Glue Data Catalog database."
  type        = string
  default     = "music_streaming_db"
}

variable "reference_bucket_name" {
  description = "Name of the reference S3 bucket that stores job scripts (from the s3 module output)."
  type        = string
}

variable "glue_role_arn" {
  description = "ARN of the Glue execution role (from the iam module output)."
  type        = string
}

variable "validation_job_name" {
  description = "Name of the Python Shell schema validation job."
  type        = string
  default     = "validation_job"
}

variable "transformation_job_name" {
  description = "Name of the Spark transformation job."
  type        = string
  default     = "transformation_job"
}

variable "dynamodb_ingestion_job_name" {
  description = "Name of the Python Shell DynamoDB ingestion job."
  type        = string
  default     = "dynamodb_ingestion_job"
}

variable "raw_bucket_name" {
  description = "Name of the raw S3 bucket, passed to the validation and transformation jobs as --raw_bucket."
  type        = string
}

variable "processed_bucket_name" {
  description = "Name of the processed S3 bucket, passed to the transformation and DynamoDB ingestion jobs as --processed_bucket."
  type        = string
}

variable "dynamodb_table_name" {
  description = "Name of the DynamoDB KPI table, passed to the DynamoDB ingestion job as --dynamodb_table."
  type        = string
}

variable "quarantine_bucket_name" {
  description = "Name of the quarantine S3 bucket, passed to the transformation job as --quarantine_bucket."
  type        = string
}

variable "tags" {
  description = "Common tags applied to all Glue jobs (Project, Environment, ManagedBy)."
  type        = map(string)
}
