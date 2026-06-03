# ---------------------------------------------------------------------------
# tests/iam.tftest.hcl
# Validates the IAM module logic with a mocked AWS provider.
# Checks that each role's trust policy names the expected AWS service principal.
# ---------------------------------------------------------------------------

mock_provider "aws" {}

run "iam_trust_policies" {
  command = plan

  module {
    source = "./modules/iam"
  }

  variables {
    name_prefix = "music-streaming-etl"
    s3_bucket_arns = [
      "arn:aws:s3:::music-streaming-etl-raw",
      "arn:aws:s3:::music-streaming-etl-reference",
      "arn:aws:s3:::music-streaming-etl-archive",
      "arn:aws:s3:::music-streaming-etl-processed",
    ]
    dynamodb_table_arn = "arn:aws:dynamodb:eu-west-1:123456789012:table/music_streaming_kpis"
    account_id         = "123456789012"
    region             = "eu-west-1"
    tags = {
      Project     = "music-streaming-etl"
      Environment = "test"
      ManagedBy   = "terraform"
    }
  }

  assert {
    condition     = jsondecode(aws_iam_role.glue.assume_role_policy).Statement[0].Principal.Service == "glue.amazonaws.com"
    error_message = "Glue role trust policy principal must be glue.amazonaws.com."
  }

  assert {
    condition     = jsondecode(aws_iam_role.step_functions.assume_role_policy).Statement[0].Principal.Service == "states.amazonaws.com"
    error_message = "Step Functions role trust policy principal must be states.amazonaws.com."
  }
}
