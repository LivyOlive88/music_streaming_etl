# ---------------------------------------------------------------------------
# modules/stepfunctions/main.tf
#
# Provisions the Step Functions state machine that orchestrates the ETL flow:
#   ListRawFiles -> ValidateSchema -> RunTransformJob -> RunDynamoIngestionJob
#   -> ArchiveFiles -> Success, with a Catch on every state routing to JobFailed.
#
# The ASL definition lives in state_machine.asl.json and is rendered with
# templatefile() so the three Glue job names (and the raw/archive bucket names)
# are injected as variables instead of being hardcoded.
# ---------------------------------------------------------------------------

resource "aws_sfn_state_machine" "etl_pipeline" {
  name     = var.state_machine_name
  role_arn = var.step_functions_role_arn

  definition = templatefile("${path.module}/state_machine.asl.json", {
    validation_job_name         = var.validation_job_name
    transformation_job_name     = var.transformation_job_name
    dynamodb_ingestion_job_name = var.dynamodb_ingestion_job_name
    raw_bucket                  = var.raw_bucket_name
    archive_bucket              = var.archive_bucket_name
  })

  tags = var.tags
}
