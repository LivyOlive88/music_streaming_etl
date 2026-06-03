# Tests — `terraform test`

This directory holds native Terraform test files (`.tftest.hcl`), one per module.
They run with a **mocked AWS provider**, so **no AWS account, credentials, or
deployed infrastructure are required**. Each test runs `terraform plan` against a
single module and asserts on critical resource attributes.

## Why no AWS account is needed

Every test file declares:

```hcl
mock_provider "aws" {}
```

`mock_provider` substitutes a fake AWS provider that returns synthetic values for
computed attributes. Combined with `command = plan`, this lets Terraform build the
resource graph and evaluate our assertions against the *configured* attributes
(encryption settings, key schema, job types, trust-policy principals, event
patterns) without ever calling AWS. This makes the suite fast, free, and safe to
run on every pull request.

## Prerequisites

- Terraform **>= 1.6.0** (native `terraform test` and `mock_provider` support).
- The AWS provider plugin, installed by `terraform init`.

## Test directory location

`terraform test` discovers `.tftest.hcl` files in the `tests/` directory **inside
the configuration directory**. These files therefore live in `terraform/tests/`
and all commands below are run from the `terraform/` directory.

```bash
cd terraform
terraform init   # one-time: installs the AWS provider plugin
```

## Run the full suite

```bash
cd terraform
terraform test
```

## Run a single module's tests

Use the `-filter` flag with the path to the test file:

```bash
cd terraform
terraform test -filter=tests/dynamodb.tftest.hcl
```

## What each file checks

| File                       | Module        | Assertions                                                        |
|----------------------------|---------------|-------------------------------------------------------------------|
| `s3.tftest.hcl`            | s3            | SSE-S3 encryption enabled, all public access blocked, EventBridge on |
| `dynamodb.tftest.hcl`      | dynamodb      | `PAY_PER_REQUEST`, `hash_key=genre`, `range_key=date`, PITR enabled |
| `iam.tftest.hcl`           | iam           | Glue and Step Functions trust-policy principals                   |
| `glue.tftest.hcl`          | glue          | Job command types (`pythonshell` / `glueetl`), `glue_version=4.0` |
| `stepfunctions.tftest.hcl` | stepfunctions | Definition non-empty, injected job names + `JobFailed` state      |
| `eventbridge.tftest.hcl`   | eventbridge   | Event pattern source/detail-type, role trust principal            |

## Coverage gate

The CI pipeline (`.github/workflows/terraform-ci.yml`) asserts that **every**
folder in `terraform/modules/` has a matching `<module>.tftest.hcl` file here. A
module without a test file fails the pipeline immediately.
