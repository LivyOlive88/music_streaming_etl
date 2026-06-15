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
├── glue_jobs/                        # Python scripts for the three Glue jobs
│   ├── validation_job.py             # Python Shell — schema-validates raw stream CSVs
│   ├── transformation_job.py         # PySpark — joins, transforms, computes KPIs
│   └── dynamodb_ingestion_job.py     # Python Shell — writes computed KPIs to DynamoDB
├── scripts/
│   └── upload_reference_data.py      # One-time helper to upload songs.csv/users.csv to S3
├── data/                             # Local source data (uploaded to the reference/raw buckets)
│   ├── songs/songs.csv               # Static song metadata (join key: track_id)
│   ├── users/users.csv               # Static user metadata (join key: user_id)
│   └── streams/streams*.csv          # Sample raw stream files
├── tests/                            # Python pytest unit tests (mocked AWS / local Spark)
│   ├── test_validation_job.py
│   ├── test_transformation_job.py
│   └── test_dynamodb_ingestion_job.py
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
7. **Python test coverage gate** — a plain shell step asserts every file in `glue_jobs/`
   has a matching `tests/test_<job>.py`, failing immediately and listing any untested jobs.
8. **flake8** — lints `glue_jobs/` and `tests/` with `--max-line-length=100`; fails on any finding.
9. **pytest** — runs the Python unit tests with coverage (`--cov=glue_jobs`) and **fails if
   coverage drops below 80%**. The runner pins **Python 3.11** and installs PySpark so
   `test_transformation_job.py` also executes here.

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

## 8. Glue Jobs

The pipeline runs three Glue jobs in sequence, orchestrated by Step Functions. Each job's
Python script lives in `glue_jobs/` and is uploaded to `s3://<reference>/scripts/<job>.py`.

### 8.1 `validation_job.py` — schema validation

| | |
|---|---|
| **Runtime** | Glue Python Shell (boto3 + pandas) |
| **Trigger** | First state in the state machine (`ValidateSchema`) |
| **Parameters** | `--raw_bucket` — name of the raw S3 bucket |
| **Reads** | The header row only of every `*.csv` in `s3://<raw>/` (ranged `get_object`) |
| **Writes** | Nothing — succeeds or raises |

**What it does:** lists every `.csv` in the raw bucket and reads only each file's header.
It checks that all three required columns (`user_id`, `track_id`, `listen_time`) are present.
If no files are found it logs a warning and exits cleanly. If any file is missing columns or
cannot be read, it raises a `ValueError` listing each failed file so Step Functions routes to
`JobFailed`.

### 8.2 `transformation_job.py` — KPI transformation

| | |
|---|---|
| **Runtime** | Glue ETL / PySpark, Glue version 4.0 |
| **Trigger** | `RunTransformJob` state |
| **Parameters** | `--JOB_NAME`, `--raw_bucket`, `--reference_bucket`, `--processed_bucket` |
| **Reads** | `s3://<raw>/*.csv` (streams), `s3://<reference>/songs.csv`, `s3://<reference>/users.csv` |
| **Writes** | `s3://<processed>/kpis/date=YYYY-MM-DD/` as Parquet (`partitionBy("date")`, overwrite) |

**What it does:** loads streams, songs and users; casts column types (dropping un-castable
rows); inner-joins streams to songs on `track_id` to add genre/duration/track metadata;
derives a `date` column from `listen_time`; computes the genre-level daily KPIs; ranks the
top 3 songs per genre/day and embeds them as an array of structs; flags the top 5 genres per
day; and writes the result as date-partitioned Parquet.

### 8.3 `dynamodb_ingestion_job.py` — DynamoDB ingestion

| | |
|---|---|
| **Runtime** | Glue Python Shell (boto3 + pandas + pyarrow) |
| **Trigger** | `RunDynamoIngestionJob` state |
| **Parameters** | `--processed_bucket`, `--dynamodb_table` |
| **Reads** | `s3://<processed>/kpis/` (all Parquet files) |
| **Writes** | One item per `(genre, date)` into the `music_streaming_kpis` DynamoDB table |

**What it does:** reads the KPI Parquet, validates the expected columns are present, reshapes
each row into a DynamoDB item (numpy types stripped, dates as `YYYY-MM-DD` strings, floats
converted to `Decimal`), and writes via `batch_writer` (25 items/call with automatic retries).
`PutItem` overwrites items with the same `(genre, date)` key, so re-running for the same date
updates the KPIs rather than duplicating them.

## 9. KPI Definitions

All KPIs are computed per **genre per day** by the transformation job.

| KPI Name | Definition | Computation |
|----------|------------|-------------|
| Listen Count | Total play events per genre per day | `count(*)` |
| Unique Listeners | Distinct users per genre per day | `countDistinct(user_id)` |
| Total Listening Time | Sum of `duration_ms` per genre per day | `sum(duration_ms)` |
| Avg Listening Time / User | Mean listening time per user per day | `total_listening_time_ms / unique_listeners` |
| Top 3 Songs per Genre | Most-played songs per genre per day | `rank()` window over `(date, genre)` by play count, `rank <= 3` |
| Top 5 Genres per Day | Genres with the highest listen count | `rank()` window over `date` by `listen_count`, `rank <= 5` (`is_top_5` flag) |

## 10. DynamoDB Access Patterns — Placeholder

> This section will be updated when the DynamoDB ingestion job is implemented.
> Will include sample query patterns and example boto3 / AWS CLI commands.
