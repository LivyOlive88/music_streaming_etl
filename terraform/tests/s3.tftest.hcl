# ---------------------------------------------------------------------------
# tests/s3.tftest.hcl
# Validates the S3 module logic with a mocked AWS provider (no real account).
# Checks that encryption is enabled and public access is fully blocked.
# ---------------------------------------------------------------------------

mock_provider "aws" {}

run "s3_security_config" {
  command = plan

  module {
    source = "./modules/s3"
  }

  variables {
    bucket_prefix = "music-streaming-etl-test"
    tags = {
      Project     = "music-streaming-etl"
      Environment = "test"
      ManagedBy   = "terraform"
    }
  }

  assert {
    # `rule` is a set, so it is not index-addressable; use one() to extract it.
    condition     = one(aws_s3_bucket_server_side_encryption_configuration.encryption["raw"].rule).apply_server_side_encryption_by_default[0].sse_algorithm == "AES256"
    error_message = "Raw bucket must use SSE-S3 (AES256) encryption."
  }

  assert {
    condition = alltrue([
      for pab in aws_s3_bucket_public_access_block.block_public :
      pab.block_public_acls && pab.block_public_policy && pab.ignore_public_acls && pab.restrict_public_buckets
    ])
    error_message = "All public access must be blocked on every bucket."
  }

  assert {
    condition     = aws_s3_bucket_notification.raw_eventbridge.eventbridge == true
    error_message = "EventBridge notifications must be enabled on the raw bucket."
  }
}
