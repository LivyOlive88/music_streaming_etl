# ---------------------------------------------------------------------------
# providers.tf
# Declares the required Terraform and AWS provider versions and configures the
# AWS provider. Default tags are applied to every taggable resource.
# ---------------------------------------------------------------------------

terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  # Apply the common tag set to every resource that supports tagging.
  default_tags {
    tags = local.common_tags
  }
}
