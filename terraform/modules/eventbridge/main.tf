# ---------------------------------------------------------------------------
# modules/eventbridge/main.tf
#
# Provisions the EventBridge rule that triggers the ETL state machine whenever
# a new .csv object is created in the raw S3 bucket. Also creates the dedicated
# IAM role EventBridge assumes to start the Step Functions execution.
#
# The raw bucket name and the state machine ARN are passed in as variables from
# the s3 and stepfunctions module outputs - nothing is hardcoded.
# ---------------------------------------------------------------------------

# Rule: match "Object Created" events from S3 for the raw bucket, .csv keys.
resource "aws_cloudwatch_event_rule" "raw_object_created" {
  name        = var.rule_name
  description = "Triggers the music streaming ETL state machine on new .csv objects in the raw bucket."

  event_pattern = jsonencode({
    source        = ["aws.s3"]
    "detail-type" = ["Object Created"]
    detail = {
      bucket = {
        name = [var.raw_bucket_name]
      }
      object = {
        key = [{ suffix = ".csv" }]
      }
    }
  })

  tags = var.tags
}

# Target: the Step Functions state machine, invoked via the EventBridge role.
resource "aws_cloudwatch_event_target" "state_machine" {
  rule     = aws_cloudwatch_event_rule.raw_object_created.name
  arn      = var.state_machine_arn
  role_arn = aws_iam_role.eventbridge.arn
}

# --------------------------- EventBridge role ------------------------------

# Trust policy as an inline jsonencode literal so the principal is known at
# plan time and assertable under `mock_provider` in terraform test.
resource "aws_iam_role" "eventbridge" {
  name = "${var.name_prefix}-eventbridge-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "EventBridgeAssumeRole"
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "events.amazonaws.com" }
    }]
  })

  tags = var.tags
}

# Allow EventBridge to start an execution of the target state machine only.
resource "aws_iam_role_policy" "start_execution" {
  name = "${var.name_prefix}-eventbridge-start-execution"
  role = aws_iam_role.eventbridge.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "StartStateMachineExecution"
      Effect   = "Allow"
      Action   = "states:StartExecution"
      Resource = var.state_machine_arn
    }]
  })
}
