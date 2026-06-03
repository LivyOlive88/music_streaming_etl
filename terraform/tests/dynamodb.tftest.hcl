# ---------------------------------------------------------------------------
# tests/dynamodb.tftest.hcl
# Validates the DynamoDB module logic with a mocked AWS provider.
# Checks billing mode, key schema, and point-in-time recovery.
# ---------------------------------------------------------------------------

mock_provider "aws" {}

run "dynamodb_table_config" {
  command = plan

  module {
    source = "./modules/dynamodb"
  }

  variables {
    table_name = "music_streaming_kpis"
    tags = {
      Project     = "music-streaming-etl"
      Environment = "test"
      ManagedBy   = "terraform"
    }
  }

  assert {
    condition     = aws_dynamodb_table.kpis.billing_mode == "PAY_PER_REQUEST"
    error_message = "DynamoDB billing mode must be PAY_PER_REQUEST."
  }

  assert {
    condition     = aws_dynamodb_table.kpis.hash_key == "genre" && aws_dynamodb_table.kpis.range_key == "date"
    error_message = "DynamoDB key schema must be hash_key=genre, range_key=date."
  }

  assert {
    condition     = aws_dynamodb_table.kpis.point_in_time_recovery[0].enabled == true
    error_message = "Point-in-time recovery must be enabled."
  }
}
