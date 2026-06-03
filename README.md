# Music Streaming ETL Pipeline — Infrastructure

## 1. Project Overview

This project provisions the AWS infrastructure for a music streaming ETL pipeline that ingests raw CSV stream files landed in Amazon S3, validates and transforms them into key listening metrics using AWS Glue (PySpark and Python Shell jobs), orchestrates the end-to-end run with AWS Step Functions, and stores the computed KPI results in Amazon DynamoDB. All infrastructure is defined and managed entirely through Terraform — no resources are created manually in the AWS console.

## 2. Architecture

```
                         (new .csv object)
   ┌──────────────┐  Object Created   ┌──────────────┐   StartExecution   ┌──────────────────┐
   │  S3 (raw/)   │ ─────────────────▶ │ EventBridge  │ ─────────────────▶ │  Step Functions  │
   └──────────────┘                    │    rule      │                    │  state machine   │
                                       └──────────────┘                    └────────┬─────────┘
                                                                                     │
                    ┌────────────────────────────────────────────────────────────────┘
                    ▼
   ┌──────────────────────┐   ┌──────────────────────┐   ┌───────────────────────────┐
   │   Validation Job     │ ▶ │  Transformation Job   │ ▶ │  DynamoDB Ingestion Job   │
   │   (Python Shell)     │   │   (PySpark / Glue)    │   │     (Python Shell)        │
   └──────────────────────┘   └──────────────────────┘   └─────────────┬─────────────┘
                                                                        │
                                                                        ▼
                                                            ┌───────────────────────┐
                                                            │  DynamoDB (KPI table) │
                                                            └───────────────────────┘
                    │
                    ▼  (on success)
            ┌──────────────┐
            │ S3 (archive/)│   final destination for processed raw files
            └──────────────┘
```

## 3. Repository Structure

```
project-root/
├── terraform/
│   ├── main.tf                       # Root module — wires all child modules together
│   ├── variables.tf                  # Root input variables
│   ├── outputs.tf                    # Aggregated outputs from all modules
│   ├── providers.tf                  # AWS provider + Terraform version constraints
│   ├── backend.tf                    # Local state backend config
│   ├── terraform.tfvars              # Non-secret variable values
│   ├── modules/
│   │   ├── s3/                        # Four buckets (raw/reference/archive/processed) + EventBridge notify
│   │   │   ├── main.tf
│   │   │   ├── variables.tf
│   │   │   └── outputs.tf
│   │   ├── dynamodb/                  # KPI table (genre/date, on-demand, PITR)
│   │   │   ├── main.tf
│   │   │   ├── variables.tf
│   │   │   └── outputs.tf
│   │   ├── iam/                       # Glue + Step Functions execution roles
│   │   │   ├── main.tf
│   │   │   ├── variables.tf
│   │   │   └── outputs.tf
│   │   ├── glue/                      # Catalog DB + 3 ETL jobs
│   │   │   ├── main.tf
│   │   │   ├── variables.tf
│   │   │   └── outputs.tf
│   │   ├── stepfunctions/             # State machine orchestrating the ETL flow
│   │   │   ├── main.tf
│   │   │   ├── variables.tf
│   │   │   ├── outputs.tf
│   │   │   └── state_machine.asl.json # ASL definition rendered via templatefile()
│   │   └── eventbridge/               # Rule + role triggering the state machine
│   │       ├── main.tf
│   │       ├── variables.tf
│   │       └── outputs.tf
│   └── tests/                         # Native `terraform test` files (mock provider)
│       ├── s3.tftest.hcl
│       ├── dynamodb.tftest.hcl
│       ├── iam.tftest.hcl
│       ├── glue.tftest.hcl
│       ├── stepfunctions.tftest.hcl
│       ├── eventbridge.tftest.hcl
│       └── README.md                  # How to run tests locally
├── README.md                          # This document
├── .tflint.hcl                        # TFLint config (AWS ruleset)
├── .gitignore                         # Ignores state files, .terraform/, etc.
└── .github/
    └── workflows/
        ├── terraform-ci.yml           # PR checks: coverage gate, fmt, validate, tflint, tfsec, test
        └── terraform-deploy.yml       # Merge-to-main deploy (apply step is a placeholder)
```

> **Note on test location:** Native `terraform test` discovers `.tftest.hcl` files
> in the `tests/` directory **inside** the configuration directory and only accepts
> a test directory within that configuration. The tests therefore live in
> `terraform/tests/` (rather than a project-root `tests/`) so `terraform test` runs
> with no extra flags.

## 4. Infrastructure Setup (Terraform)

**Prerequisites**

- Terraform **>= 1.6.0**
- AWS CLI configured with credentials (`aws configure`) for an account/region you control
- (Tests only) Terraform 1.6+ — the test suite uses a mocked provider, so no AWS account is needed to run it

**First-time setup**

```bash
# 1. Clone the repository and enter the Terraform directory
git clone <your-repo-url>
cd <repo>/terraform

# 2. Initialize providers and the local backend
terraform init

# 3. Review the execution plan
terraform plan

# 4. Apply to create the infrastructure
terraform apply
```

**Tear down**

```bash
cd terraform
terraform destroy
```

> ⚠️ **State is local.** `terraform.tfstate` (and its backup) live on your machine
> and are **gitignored**. Never commit them — they can contain sensitive resource
> details.

## 5. AWS Resources Provisioned

| Resource | Module | Purpose |
|----------|--------|---------|
| 4 × S3 bucket (raw, reference, archive, processed) | `s3` | Landing, scripts/reference data, archive of processed files, processed output |
| S3 public access blocks, SSE-S3 encryption, versioning, lifecycle rules | `s3` | Security & data lifecycle (raw → IA @30d, expire @90d) |
| S3 bucket notification (EventBridge) | `s3` | Publishes "Object Created" events from the raw bucket |
| DynamoDB table `music_streaming_kpis` | `dynamodb` | Stores KPI results (PK `genre`, SK `date`, on-demand, PITR) |
| IAM role (Glue execution) + inline policy | `iam` | Glue job access to S3, DynamoDB, CloudWatch Logs |
| IAM role (Step Functions execution) + inline policy | `iam` | Start/monitor Glue jobs; list pipeline buckets |
| Glue catalog database `music_streaming_db` | `glue` | Metadata catalog for the pipeline |
| 3 × Glue job (validation, transformation, dynamodb_ingestion) | `glue` | Python Shell + PySpark ETL jobs |
| Step Functions state machine | `stepfunctions` | Orchestrates ListRawFiles → Validate → Transform → Ingest → Archive |
| EventBridge rule + target | `eventbridge` | Triggers the state machine on new raw `.csv` objects |
| IAM role (EventBridge) + inline policy | `eventbridge` | Allows EventBridge to start the state machine execution |

## 6. CI Pipeline

A GitHub Actions workflow (`.github/workflows/terraform-ci.yml`) runs on **every pull
request targeting `main`** and enforces these checks in order, failing fast:

1. **Test coverage gate** — a plain shell step asserts every folder under
   `terraform/modules/` has a matching `<module>.tftest.hcl` in `terraform/tests/`,
   listing any untested modules and failing immediately if so.
2. **`terraform fmt -check -recursive`** — fails if any `.tf` file is unformatted.
3. **`terraform validate`** — initialized with a mocked backend (`-backend=false`).
4. **tflint** — lints all modules against the AWS ruleset (`.tflint.hcl`).
5. **tfsec** — security scan; **fails on HIGH/CRITICAL** findings, MEDIUM are warnings only.
6. **`terraform test`** — native tests with mock providers (no AWS account needed).

Deployment is **not** run on PRs. On **merge to `main`**,
`.github/workflows/terraform-deploy.yml` is the deployment workflow; its
`terraform apply` step is currently a commented placeholder pending a manual
approval gate.

## 7. Testing

Tests use **native `terraform test` with mock providers** — they run `terraform plan`
against each module with a fake AWS provider, so **no AWS account or credentials are
required**. They run locally and on every PR.

```bash
cd terraform
terraform test                                  # full suite
terraform test -filter=tests/dynamodb.tftest.hcl  # single module
```

See [terraform/tests/README.md](terraform/tests/README.md) for full setup and run instructions.

## 8. Glue Jobs — Placeholder

> This section will be updated when Glue job scripts are added.
> Jobs to be documented: validation_job, transformation_job, dynamodb_ingestion_job.

## 9. KPI Definitions — Placeholder

> This section will be updated when the transformation logic is implemented.
> KPIs to be documented: Listen Count, Unique Listeners, Total Listening Time,
> Average Listening Time per User, Top 3 Songs per Genre per Day, Top 5 Genres per Day.

## 10. DynamoDB Access Patterns — Placeholder

> This section will be updated when the DynamoDB ingestion job is implemented.
> Will include sample query patterns and example boto3 / AWS CLI commands.
