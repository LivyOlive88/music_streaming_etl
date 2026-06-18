# ---------------------------------------------------------------------------
# modules/s3/main.tf
#
# Provisions the four S3 buckets used by the music streaming ETL pipeline:
#   - raw        : landing zone for incoming CSV stream files (EventBridge source)
#   - reference  : holds Glue job scripts and reference/lookup data
#   - archive    : final destination for successfully processed raw files
#   - processed  : intermediate/processed output written by the ETL jobs
#
# Every bucket is private (all public access blocked) and encrypted with SSE-S3.
# Versioning is enabled on the raw and reference buckets. The raw bucket has a
# lifecycle policy and emits Object Created events to EventBridge.
# ---------------------------------------------------------------------------

locals {
  # Map of logical bucket name -> concrete bucket name (<prefix>-<logical>).
  buckets = {
    raw        = "${var.bucket_prefix}-raw"
    reference  = "${var.bucket_prefix}-reference"
    archive    = "${var.bucket_prefix}-archive"
    processed  = "${var.bucket_prefix}-processed"
    quarantine = "${var.bucket_prefix}-quarantine"
  }

  # Buckets that require object versioning enabled.
  versioned_buckets = ["raw", "reference"]
}

# The four pipeline buckets.
resource "aws_s3_bucket" "buckets" {
  for_each = local.buckets

  bucket = each.value
  tags   = merge(var.tags, { Name = each.value })
}

# Block ALL public access on every bucket.
resource "aws_s3_bucket_public_access_block" "block_public" {
  for_each = aws_s3_bucket.buckets

  bucket = each.value.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Server-side encryption (SSE-S3 / AES256) on every bucket.
resource "aws_s3_bucket_server_side_encryption_configuration" "encryption" {
  for_each = aws_s3_bucket.buckets

  bucket = each.value.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

# Versioning on the raw and reference buckets only.
resource "aws_s3_bucket_versioning" "versioning" {
  for_each = toset(local.versioned_buckets)

  bucket = aws_s3_bucket.buckets[each.key].id

  versioning_configuration {
    status = "Enabled"
  }
}

# Lifecycle rules on the raw bucket:
#   - transition objects to STANDARD_IA after 30 days
#   - expire objects after 90 days
resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  bucket = aws_s3_bucket.buckets["raw"].id

  # Depend on versioning so the configuration applies cleanly to a versioned bucket.
  depends_on = [aws_s3_bucket_versioning.versioning]

  rule {
    id     = "raw-transition-and-expire"
    status = "Enabled"

    filter {
      prefix = ""
    }

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    expiration {
      days = 90
    }
  }
}

# Enable EventBridge notifications on the raw bucket so S3 publishes
# "Object Created" events to the default EventBridge bus automatically.
resource "aws_s3_bucket_notification" "raw_eventbridge" {
  bucket      = aws_s3_bucket.buckets["raw"].id
  eventbridge = true
}

# ---------------------------------------------------------------------------
# Reference data + Glue job scripts.
#
# The reference datasets are uploaded to the root of the reference bucket; the
# three Glue job scripts are uploaded under a scripts/ prefix (matching the
# script_location values in modules/glue/main.tf). The etag (filemd5) means
# Terraform re-uploads a file only when its content changes.
# ---------------------------------------------------------------------------

resource "aws_s3_object" "songs" {
  bucket = aws_s3_bucket.buckets["reference"].bucket
  key    = "songs.csv"
  source = "${path.module}/../../../data/songs/songs.csv"
  etag   = filemd5("${path.module}/../../../data/songs/songs.csv")
}

resource "aws_s3_object" "users" {
  bucket = aws_s3_bucket.buckets["reference"].bucket
  key    = "users.csv"
  source = "${path.module}/../../../data/users/users.csv"
  etag   = filemd5("${path.module}/../../../data/users/users.csv")
}

# The three Glue job scripts, uploaded under scripts/ in the reference bucket.
locals {
  glue_job_scripts = [
    "validation_job",
    "transformation_job",
    "dynamodb_ingestion_job",
  ]
}

resource "aws_s3_object" "glue_scripts" {
  for_each = toset(local.glue_job_scripts)

  bucket = aws_s3_bucket.buckets["reference"].bucket
  key    = "scripts/${each.key}.py"
  source = "${path.module}/../../../glue_jobs/${each.key}.py"
  etag   = filemd5("${path.module}/../../../glue_jobs/${each.key}.py")
}
