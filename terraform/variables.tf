# ---------------------------------------------------------------------------
# variables.tf
# Root-module input variables. Concrete values are supplied in terraform.tfvars.
# ---------------------------------------------------------------------------

variable "aws_region" {
  description = "AWS region into which all infrastructure is deployed."
  type        = string
  default     = "eu-west-1"
}

variable "project_name" {
  description = "Project identifier, used for tagging and resource name prefixes."
  type        = string
  default     = "music-streaming-etl"
}

variable "environment" {
  description = "Deployment environment (e.g. dev, test, prod)."
  type        = string
  default     = "dev"
}

variable "bucket_prefix" {
  description = "Globally unique prefix for the four S3 bucket names (e.g. <prefix>-raw)."
  type        = string
  default     = "music-streaming-etl"
}

variable "dynamodb_table_name" {
  description = "Name of the DynamoDB table that stores KPI results."
  type        = string
  default     = "music_streaming_kpis"
}

variable "glue_database_name" {
  description = "Name of the Glue Data Catalog database."
  type        = string
  default     = "music_streaming_db"
}

variable "tags" {
  description = "Additional tags merged into the common tag set applied to all resources."
  type        = map(string)
  default     = {}
}
