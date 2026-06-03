# ---------------------------------------------------------------------------
# tests/glue.tftest.hcl
# Validates the Glue module logic with a mocked AWS provider.
# Checks job command types and the Spark job's glue_version.
# ---------------------------------------------------------------------------

mock_provider "aws" {}

run "glue_job_config" {
  command = plan

  module {
    source = "./modules/glue"
  }

  variables {
    database_name         = "music_streaming_db"
    reference_bucket_name = "music-streaming-etl-reference"
    glue_role_arn         = "arn:aws:iam::123456789012:role/music-streaming-etl-glue-execution-role"
    tags = {
      Project     = "music-streaming-etl"
      Environment = "test"
      ManagedBy   = "terraform"
    }
  }

  assert {
    condition     = aws_glue_job.validation.command[0].name == "pythonshell" && aws_glue_job.dynamodb_ingestion.command[0].name == "pythonshell"
    error_message = "Validation and DynamoDB ingestion jobs must be of type pythonshell."
  }

  assert {
    condition     = aws_glue_job.transformation.command[0].name == "glueetl" && aws_glue_job.transformation.glue_version == "4.0"
    error_message = "Transformation job must be glueetl on Glue version 4.0."
  }
}
