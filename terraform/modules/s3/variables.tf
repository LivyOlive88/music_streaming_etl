# ---------------------------------------------------------------------------
# modules/s3/variables.tf
# Input variables for the S3 module.
# ---------------------------------------------------------------------------

variable "bucket_prefix" {
  description = "Prefix applied to every bucket name (e.g. \"music-streaming-etl\" yields \"music-streaming-etl-raw\"). Must be globally unique across AWS."
  type        = string
}

variable "tags" {
  description = "Common tags applied to all buckets (Project, Environment, ManagedBy)."
  type        = map(string)
}
