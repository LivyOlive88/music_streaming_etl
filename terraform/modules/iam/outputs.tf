# ---------------------------------------------------------------------------
# modules/iam/outputs.tf
# Exposes both execution role ARNs for the glue and stepfunctions modules.
# ---------------------------------------------------------------------------

output "glue_role_arn" {
  description = "ARN of the Glue execution role."
  value       = aws_iam_role.glue.arn
}

output "glue_role_name" {
  description = "Name of the Glue execution role."
  value       = aws_iam_role.glue.name
}

output "step_functions_role_arn" {
  description = "ARN of the Step Functions execution role."
  value       = aws_iam_role.step_functions.arn
}

output "step_functions_role_name" {
  description = "Name of the Step Functions execution role."
  value       = aws_iam_role.step_functions.name
}
