# ---------------------------------------------------------------------------
# providers.tf
# Declares the required Terraform and AWS provider versions and configures the
# AWS provider. Default tags are applied to every taggable resource.
# ---------------------------------------------------------------------------
provider "aws" {
  region = var.aws_region

  # Apply the common tag set to every resource that supports tagging.
  default_tags {
    tags = local.common_tags
  }
}
