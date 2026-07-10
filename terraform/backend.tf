# ---------------------------------------------------------------------------
# backend.tf
# Configures remote state storage in S3, with a DynamoDB table for locking.
# ---------------------------------------------------------------------------

terraform {
  backend "s3" {
    bucket         = "music-streaming-etl-tfstate-821528308689"
    key            = "terraform.tfstate"
    region         = "eu-west-1"
    dynamodb_table = "music-streaming-etl-tfstate-lock"
    encrypt        = true
  }
}
