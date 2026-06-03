# ---------------------------------------------------------------------------
# modules/eventbridge/outputs.tf
# Exposes the rule ARN and name.
# ---------------------------------------------------------------------------

output "rule_arn" {
  description = "ARN of the EventBridge rule."
  value       = aws_cloudwatch_event_rule.raw_object_created.arn
}

output "rule_name" {
  description = "Name of the EventBridge rule."
  value       = aws_cloudwatch_event_rule.raw_object_created.name
}

output "eventbridge_role_arn" {
  description = "ARN of the IAM role EventBridge assumes to start the state machine."
  value       = aws_iam_role.eventbridge.arn
}
