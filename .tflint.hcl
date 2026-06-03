# ---------------------------------------------------------------------------
# .tflint.hcl
# TFLint configuration. Enables the Terraform core ruleset and the AWS ruleset
# so modules are linted against AWS best practices.
# ---------------------------------------------------------------------------

config {
  call_module_type = "all"
}

plugin "terraform" {
  enabled = true
  preset  = "recommended"
}

plugin "aws" {
  enabled = true
  version = "0.39.0"
  source  = "github.com/terraform-linters/tflint-ruleset-aws"
}
