# ---------------------------------------------------------------------------
# tests/stepfunctions.tftest.hcl
# Validates the Step Functions module logic with a mocked AWS provider.
# Checks that the rendered ASL definition is non-empty and contains the
# expected states with injected (not hardcoded) Glue job names.
# ---------------------------------------------------------------------------

mock_provider "aws" {}

run "state_machine_definition" {
  command = plan

  module {
    source = "./modules/stepfunctions"
  }

  variables {
    state_machine_name          = "music_streaming_etl"
    step_functions_role_arn     = "arn:aws:iam::123456789012:role/music-streaming-etl-stepfunctions-execution-role"
    validation_job_name         = "validation_job"
    transformation_job_name     = "transformation_job"
    dynamodb_ingestion_job_name = "dynamodb_ingestion_job"
    raw_bucket_name             = "music-streaming-etl-raw"
    archive_bucket_name         = "music-streaming-etl-archive"
    tags = {
      Project     = "music-streaming-etl"
      Environment = "test"
      ManagedBy   = "terraform"
    }
  }

  assert {
    condition     = length(aws_sfn_state_machine.etl_pipeline.definition) > 0
    error_message = "State machine definition must not be empty."
  }

  assert {
    condition     = strcontains(aws_sfn_state_machine.etl_pipeline.definition, "transformation_job") && strcontains(aws_sfn_state_machine.etl_pipeline.definition, "JobFailed")
    error_message = "ASL must reference the injected Glue job name and define the JobFailed terminal state."
  }
}
