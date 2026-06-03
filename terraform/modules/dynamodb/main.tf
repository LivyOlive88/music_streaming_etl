# ---------------------------------------------------------------------------
# modules/dynamodb/main.tf
#
# Provisions the DynamoDB table that stores computed KPI results for the music
# streaming pipeline. The table is keyed by genre (partition) and date (sort),
# billed on-demand (PAY_PER_REQUEST), encrypted, and protected with
# point-in-time recovery.
# ---------------------------------------------------------------------------

resource "aws_dynamodb_table" "kpis" {
  name         = var.table_name
  billing_mode = "PAY_PER_REQUEST"

  hash_key  = "genre"
  range_key = "date"

  attribute {
    name = "genre"
    type = "S"
  }

  attribute {
    name = "date"
    type = "S"
  }

  # Continuous backups / restore to any point in the last 35 days.
  point_in_time_recovery {
    enabled = true
  }

  # Encrypt at rest using the AWS-owned key (no extra cost).
  server_side_encryption {
    enabled = true
  }

  tags = merge(var.tags, { Name = var.table_name })
}
