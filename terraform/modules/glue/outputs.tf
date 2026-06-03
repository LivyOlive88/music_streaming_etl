# ---------------------------------------------------------------------------
# modules/glue/outputs.tf
# Exposes the database name and all three job names so the stepfunctions module
# can reference them without hardcoding.
# ---------------------------------------------------------------------------

output "database_name" {
  description = "Name of the Glue Data Catalog database."
  value       = aws_glue_catalog_database.database.name
}

output "validation_job_name" {
  description = "Name of the schema validation Glue job."
  value       = aws_glue_job.validation.name
}

output "transformation_job_name" {
  description = "Name of the transformation Glue job."
  value       = aws_glue_job.transformation.name
}

output "dynamodb_ingestion_job_name" {
  description = "Name of the DynamoDB ingestion Glue job."
  value       = aws_glue_job.dynamodb_ingestion.name
}

output "job_names" {
  description = "List of all three Glue job names."
  value = [
    aws_glue_job.validation.name,
    aws_glue_job.transformation.name,
    aws_glue_job.dynamodb_ingestion.name,
  ]
}
