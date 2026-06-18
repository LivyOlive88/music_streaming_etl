# ---------------------------------------------------------------------------
# modules/s3/outputs.tf
# Exposes bucket names and ARNs so dependent modules (iam, glue, eventbridge)
# can consume them without hardcoding.
# ---------------------------------------------------------------------------

output "bucket_names" {
  description = "Map of logical bucket key (raw/reference/archive/processed) to concrete bucket name."
  value       = { for k, b in aws_s3_bucket.buckets : k => b.id }
}

output "bucket_arns" {
  description = "Map of logical bucket key (raw/reference/archive/processed) to bucket ARN."
  value       = { for k, b in aws_s3_bucket.buckets : k => b.arn }
}

output "raw_bucket_name" {
  description = "Name of the raw landing bucket (EventBridge source)."
  value       = aws_s3_bucket.buckets["raw"].id
}

output "raw_bucket_arn" {
  description = "ARN of the raw landing bucket."
  value       = aws_s3_bucket.buckets["raw"].arn
}

output "reference_bucket_name" {
  description = "Name of the reference bucket that stores Glue job scripts."
  value       = aws_s3_bucket.buckets["reference"].id
}

output "quarantine_bucket_name" {
  description = "Name of the quarantine bucket that stores rows rejected by data quality checks."
  value       = aws_s3_bucket.buckets["quarantine"].id
}

output "all_bucket_arns" {
  description = "List of all five bucket ARNs (raw/reference/archive/processed/quarantine), for IAM policy statements."
  value       = [for b in aws_s3_bucket.buckets : b.arn]
}
