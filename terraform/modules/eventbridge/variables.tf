# ---------------------------------------------------------------------------
# modules/eventbridge/variables.tf
# Input variables for the EventBridge module.
# ---------------------------------------------------------------------------

variable "rule_name" {
  description = "Name of the EventBridge rule."
  type        = string
  default     = "music_streaming_raw_csv_created"
}

variable "name_prefix" {
  description = "Prefix applied to the EventBridge IAM role and policy names."
  type        = string
}

variable "raw_bucket_name" {
  description = "Name of the raw S3 bucket to filter events on (from the s3 module output)."
  type        = string
}

variable "state_machine_arn" {
  description = "ARN of the Step Functions state machine to trigger (from the stepfunctions module output)."
  type        = string
}

variable "tags" {
  description = "Common tags applied to the rule and role (Project, Environment, ManagedBy)."
  type        = map(string)
}
