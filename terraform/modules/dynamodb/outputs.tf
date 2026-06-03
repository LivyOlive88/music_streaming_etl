# ---------------------------------------------------------------------------
# modules/dynamodb/outputs.tf
# Exposes the table name and ARN for the IAM module to grant access.
# ---------------------------------------------------------------------------

output "table_name" {
  description = "Name of the KPI DynamoDB table."
  value       = aws_dynamodb_table.kpis.name
}

output "table_arn" {
  description = "ARN of the KPI DynamoDB table."
  value       = aws_dynamodb_table.kpis.arn
}
