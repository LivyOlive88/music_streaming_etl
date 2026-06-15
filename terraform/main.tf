# ---------------------------------------------------------------------------
# main.tf
# Root module. Wires the child modules together, passing each module's outputs
# into the modules that depend on them so nothing is hardcoded. The dependency
# order is: s3 + dynamodb -> iam -> glue -> stepfunctions -> eventbridge.
# ---------------------------------------------------------------------------

# Account ID and region are resolved at runtime - never hardcoded.
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  # Common tag set applied (via provider default_tags) to every resource.
  common_tags = merge(
    {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "terraform"
    },
    var.tags,
  )
}

# S3 buckets (raw, reference, archive, processed) + EventBridge notifications.
module "s3" {
  source = "./modules/s3"

  bucket_prefix = var.bucket_prefix
  tags          = local.common_tags
}

# DynamoDB KPI table.
module "dynamodb" {
  source = "./modules/dynamodb"

  table_name = var.dynamodb_table_name
  tags       = local.common_tags
}

# IAM execution roles for Glue and Step Functions.
module "iam" {
  source = "./modules/iam"

  name_prefix        = var.project_name
  s3_bucket_arns     = module.s3.all_bucket_arns
  dynamodb_table_arn = module.dynamodb.table_arn
  account_id         = data.aws_caller_identity.current.account_id
  region             = data.aws_region.current.name
  tags               = local.common_tags
}

# Glue catalog database and the three ETL jobs.
module "glue" {
  source = "./modules/glue"

  database_name         = var.glue_database_name
  reference_bucket_name = module.s3.reference_bucket_name
  raw_bucket_name       = module.s3.raw_bucket_name
  processed_bucket_name = module.s3.bucket_names["processed"]
  dynamodb_table_name   = var.dynamodb_table_name
  glue_role_arn         = module.iam.glue_role_arn
  tags                  = local.common_tags
}

# Step Functions state machine orchestrating the ETL flow.
module "stepfunctions" {
  source = "./modules/stepfunctions"

  step_functions_role_arn     = module.iam.step_functions_role_arn
  validation_job_name         = module.glue.validation_job_name
  transformation_job_name     = module.glue.transformation_job_name
  dynamodb_ingestion_job_name = module.glue.dynamodb_ingestion_job_name
  raw_bucket_name             = module.s3.raw_bucket_name
  archive_bucket_name         = module.s3.bucket_names["archive"]
  tags                        = local.common_tags
}

# EventBridge rule + role triggering the state machine on new raw .csv objects.
module "eventbridge" {
  source = "./modules/eventbridge"

  name_prefix       = var.project_name
  raw_bucket_name   = module.s3.raw_bucket_name
  state_machine_arn = module.stepfunctions.state_machine_arn
  tags              = local.common_tags
}
