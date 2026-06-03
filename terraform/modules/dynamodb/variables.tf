# ---------------------------------------------------------------------------
# modules/dynamodb/variables.tf
# Input variables for the DynamoDB module.
# ---------------------------------------------------------------------------

variable "table_name" {
  description = "Name of the DynamoDB table that holds KPI results."
  type        = string
  default     = "music_streaming_kpis"
}

variable "tags" {
  description = "Common tags applied to the table (Project, Environment, ManagedBy)."
  type        = map(string)
}
