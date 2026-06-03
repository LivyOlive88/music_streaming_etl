# ---------------------------------------------------------------------------
# modules/stepfunctions/outputs.tf
# Exposes the state machine ARN and name for the eventbridge module.
# ---------------------------------------------------------------------------

output "state_machine_arn" {
  description = "ARN of the ETL state machine."
  value       = aws_sfn_state_machine.etl_pipeline.arn
}

output "state_machine_name" {
  description = "Name of the ETL state machine."
  value       = aws_sfn_state_machine.etl_pipeline.name
}
