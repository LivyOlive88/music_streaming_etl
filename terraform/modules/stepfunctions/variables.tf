# ---------------------------------------------------------------------------
# modules/stepfunctions/variables.tf
# Input variables for the Step Functions module.
# ---------------------------------------------------------------------------

variable "state_machine_name" {
  description = "Name of the Step Functions state machine."
  type        = string
  default     = "music_streaming_etl"
}

variable "step_functions_role_arn" {
  description = "ARN of the Step Functions execution role (from the iam module output)."
  type        = string
}

variable "validation_job_name" {
  description = "Name of the schema validation Glue job (from the glue module output)."
  type        = string
}

variable "transformation_job_name" {
  description = "Name of the transformation Glue job (from the glue module output)."
  type        = string
}

variable "dynamodb_ingestion_job_name" {
  description = "Name of the DynamoDB ingestion Glue job (from the glue module output)."
  type        = string
}

variable "raw_bucket_name" {
  description = "Name of the raw S3 bucket, used by the ListRawFiles state."
  type        = string
}

variable "archive_bucket_name" {
  description = "Name of the archive S3 bucket, used by the ArchiveFiles state."
  type        = string
}

variable "tags" {
  description = "Common tags applied to the state machine (Project, Environment, ManagedBy)."
  type        = map(string)
}
