# ---------------------------------------------------------------------------
# modules/iam/main.tf
#
# Provisions the execution roles used by the pipeline:
#   - Glue execution role          : assumed by AWS Glue jobs. Grants access to
#                                     the pipeline S3 buckets, the KPI DynamoDB
#                                     table and CloudWatch Logs.
#   - Step Functions execution role: assumed by the state machine. Grants
#                                     permission to start and monitor Glue jobs.
#
# All resource ARNs are passed in from other module outputs — nothing is
# hardcoded here.
# ---------------------------------------------------------------------------

# Trust policies are written as inline jsonencode literals so their principal is
# known at plan time (data-source policy documents resolve to mocked values
# under `mock_provider`, which would make them unassertable in terraform test).
locals {
  glue_assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "GlueAssumeRole"
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })

  sfn_assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "StepFunctionsAssumeRole"
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "states.amazonaws.com" }
    }]
  })
}

# -------------------------- Glue execution role ----------------------------

resource "aws_iam_role" "glue" {
  name               = "${var.name_prefix}-glue-execution-role"
  assume_role_policy = local.glue_assume_role_policy
  tags               = var.tags
}

# AWS-managed baseline policy for Glue.
resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Custom inline policy: S3 data access, DynamoDB writes/reads, CloudWatch Logs.
# Written as jsonencode (not a data source) so it is fully known at plan time and
# survives `mock_provider` in terraform test.
resource "aws_iam_role_policy" "glue_inline" {
  name = "${var.name_prefix}-glue-inline-policy"
  role = aws_iam_role.glue.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3DataAccess"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
        ]
        # Bucket-level ARNs (for ListBucket) and object-level ARNs (/*).
        Resource = concat(
          var.s3_bucket_arns,
          [for arn in var.s3_bucket_arns : "${arn}/*"],
        )
      },
      {
        Sid    = "DynamoDBAccess"
        Effect = "Allow"
        Action = [
          "dynamodb:PutItem",
          "dynamodb:BatchWriteItem",
          "dynamodb:GetItem",
          "dynamodb:Query",
        ]
        Resource = var.dynamodb_table_arn
      },
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
        ]
        Resource = "arn:aws:logs:${var.region}:${var.account_id}:*"
      },
    ]
  })
}

# --------------------- Step Functions execution role -----------------------

resource "aws_iam_role" "step_functions" {
  name               = "${var.name_prefix}-stepfunctions-execution-role"
  assume_role_policy = local.sfn_assume_role_policy
  tags               = var.tags
}

# Allow the state machine to start and monitor Glue job runs, plus the minimal
# S3 list access the ListRawFiles / ArchiveFiles Task states require.
resource "aws_iam_role_policy" "sfn_inline" {
  name = "${var.name_prefix}-stepfunctions-inline-policy"
  role = aws_iam_role.step_functions.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "GlueJobControl"
        Effect = "Allow"
        Action = [
          "glue:StartJobRun",
          "glue:GetJobRun",
          "glue:GetJobRuns",
        ]
        Resource = "arn:aws:glue:${var.region}:${var.account_id}:job/*"
      },
      # Required by the ListRawFiles / ArchiveFiles states in the ASL definition,
      # which call the S3 ListObjectsV2 SDK integration on the pipeline buckets.
      {
        Sid      = "S3ListForOrchestration"
        Effect   = "Allow"
        Action   = "s3:ListBucket"
        Resource = var.s3_bucket_arns
      },
    ]
  })
}
