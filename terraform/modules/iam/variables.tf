# ---------------------------------------------------------------------------
# modules/iam/variables.tf
# Input variables for the IAM module.
# ---------------------------------------------------------------------------

variable "name_prefix" {
  description = "Prefix applied to IAM role and policy names."
  type        = string
}

variable "s3_bucket_arns" {
  description = "List of the four pipeline S3 bucket ARNs (from the s3 module output)."
  type        = list(string)
}

variable "dynamodb_table_arn" {
  description = "ARN of the KPI DynamoDB table (from the dynamodb module output)."
  type        = string
}

variable "account_id" {
  description = "AWS account ID, used to scope CloudWatch Logs and Glue ARNs (no hardcoding)."
  type        = string
}

variable "region" {
  description = "AWS region, used to scope CloudWatch Logs and Glue ARNs (no hardcoding)."
  type        = string
}

variable "tags" {
  description = "Common tags applied to the IAM roles (Project, Environment, ManagedBy)."
  type        = map(string)
}
