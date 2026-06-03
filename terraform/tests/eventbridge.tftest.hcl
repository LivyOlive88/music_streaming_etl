# ---------------------------------------------------------------------------
# tests/eventbridge.tftest.hcl
# Validates the EventBridge module logic with a mocked AWS provider.
# Checks the event pattern source/detail-type and the role trust principal.
# ---------------------------------------------------------------------------

mock_provider "aws" {}

run "eventbridge_rule_config" {
  command = plan

  module {
    source = "./modules/eventbridge"
  }

  variables {
    rule_name         = "music_streaming_raw_csv_created"
    name_prefix       = "music-streaming-etl"
    raw_bucket_name   = "music-streaming-etl-raw"
    state_machine_arn = "arn:aws:states:eu-west-1:123456789012:stateMachine:music_streaming_etl"
    tags = {
      Project     = "music-streaming-etl"
      Environment = "test"
      ManagedBy   = "terraform"
    }
  }

  assert {
    condition     = jsondecode(aws_cloudwatch_event_rule.raw_object_created.event_pattern).source[0] == "aws.s3"
    error_message = "Event pattern source must be aws.s3."
  }

  assert {
    condition     = jsondecode(aws_cloudwatch_event_rule.raw_object_created.event_pattern)["detail-type"][0] == "Object Created"
    error_message = "Event pattern detail-type must be 'Object Created'."
  }

  assert {
    condition     = jsondecode(aws_iam_role.eventbridge.assume_role_policy).Statement[0].Principal.Service == "events.amazonaws.com"
    error_message = "EventBridge role trust policy principal must be events.amazonaws.com."
  }
}
