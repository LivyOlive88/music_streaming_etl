# ---------------------------------------------------------------------------
# outputs.tf
# Root-module outputs aggregating the key identifiers from every child module.
# ---------------------------------------------------------------------------

output "s3_bucket_names" {
  description = "Map of logical bucket key to bucket name."
  value       = module.s3.bucket_names
}

output "s3_bucket_arns" {
  description = "Map of logical bucket key to bucket ARN."
  value       = module.s3.bucket_arns
}

output "dynamodb_table_name" {
  description = "Name of the KPI DynamoDB table."
  value       = module.dynamodb.table_name
}

output "dynamodb_table_arn" {
  description = "ARN of the KPI DynamoDB table."
  value       = module.dynamodb.table_arn
}

output "glue_role_arn" {
  description = "ARN of the Glue execution role."
  value       = module.iam.glue_role_arn
}

output "step_functions_role_arn" {
  description = "ARN of the Step Functions execution role."
  value       = module.iam.step_functions_role_arn
}

output "glue_database_name" {
  description = "Name of the Glue Data Catalog database."
  value       = module.glue.database_name
}

output "glue_job_names" {
  description = "List of all three Glue job names."
  value       = module.glue.job_names
}

output "state_machine_arn" {
  description = "ARN of the ETL state machine."
  value       = module.stepfunctions.state_machine_arn
}

output "state_machine_name" {
  description = "Name of the ETL state machine."
  value       = module.stepfunctions.state_machine_name
}

output "eventbridge_rule_arn" {
  description = "ARN of the EventBridge rule that triggers the pipeline."
  value       = module.eventbridge.rule_arn
}

output "eventbridge_rule_name" {
  description = "Name of the EventBridge rule that triggers the pipeline."
  value       = module.eventbridge.rule_name
}
