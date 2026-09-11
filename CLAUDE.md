# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A small serverless pipeline (despite the repo name "reddit-pipeline") that pulls the current top 10
Hacker News stories every hour, validates/transforms them, and lands them in both S3 (as CSV) and a
Postgres database.

```
EventBridge Scheduler (hourly)
        |
        v
 hackernews-fetch Lambda ---> S3 (raw bucket, hackernews/.../raw.json)
                                       |
                                       | S3 ObjectCreated event
                                       v
                              hackernews-process Lambda
                                 |            |
                                 v            v
                    S3 (processed bucket,   RDS Postgres
                    .../processed.csv)      (hackernews_posts table)
                                 |
                                 v (on data quality issues)
                            SNS -> email alert
```

- `fetch/fetch_hackernews.py` — Lambda: calls the HN API for the top 10 stories, writes raw JSON to
  the raw S3 bucket. Triggered hourly by EventBridge.
- `process/process_hackernews.py` — Lambda: triggered by the S3 write. Transforms each story into a
  flat record, runs data-quality checks (missing fields, invalid scores/ranks, oversized titles),
  sends an SNS alert for any invalid posts, writes valid posts as CSV to the processed S3 bucket, and
  upserts them into `hackernews_posts` in RDS (deduplicated on `post_id` + `fetched_at`).
- `terraform/` — all AWS infrastructure as code (S3 buckets, RDS, IAM, SNS topic, Secrets Manager
  secret, EventBridge schedule). See `terraform/README.md`.
- `tests/` — pytest suite for both Lambdas, with all AWS/HN calls mocked.
- `.github/workflows/deploy.yml` — CI: runs tests, then deploys both Lambda function *code* on push to
  `main`.

## Commands

```bash
# setup
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt

# run the full suite
.venv/bin/pytest tests/ -v

# run a single test file / test
.venv/bin/pytest tests/test_process_hackernews.py -v
.venv/bin/pytest tests/test_process_hackernews.py::test_validate_post_flags_missing_post_id -v
```

Tests mock all AWS calls (S3, SNS, Secrets Manager) and the HN API, so no credentials are needed.
`psycopg2` is stubbed automatically in `tests/conftest.py` when it isn't installed locally — in Lambda
it's provided by a separate layer (`psycopg2-layer/`) rather than pip-installed, since its binary
wheel isn't available on every dev/CI platform.

```bash
# terraform (manual only — never run from CI)
cd terraform
terraform init
terraform plan
terraform apply
```

## Architecture notes

- **Two independently deployed Lambdas, one repo.** `fetch/` and `process/` are each zipped and
  deployed separately by CI (`deploy-fetch` / `deploy-process` jobs in `deploy.yml`), gated on the
  shared test suite passing. `tests/conftest.py` puts both `fetch/` and `process/` on `sys.path` so
  the test suite can import from either without them being packages.
- **Code vs. infrastructure are deployed by different mechanisms, deliberately.** CI (on every push to
  `main`) deploys Lambda *code only* via `aws lambda update-function-code`. Terraform owns everything
  else (sizing, IAM, buckets, RDS, schedule) and is applied manually, never from CI — infra changes get
  reviewed before touching real resources. In `terraform/lambda.tf`, `filename` /
  `source_code_hash` on both `aws_lambda_function` resources are explicitly ignored so a `terraform
  plan` never conflicts with what CI already deployed. Don't try to make Terraform manage Lambda code,
  and don't add Lambda code deploys to the Terraform flow.
- **Deploys use GitHub OIDC**, not long-lived AWS keys — `deploy.yml` assumes
  `arn:aws:iam::360934290883:role/github-actions-hackernews-pipeline-deploy` via
  `aws-actions/configure-aws-credentials`.
- **DB credentials live in Secrets Manager**, fetched and cached per-container in
  `process_hackernews.py` (`get_db_credentials`, module-level `_db_credentials` cache) — never passed
  as plain Lambda env vars, since those are visible to anyone with read access to the function config.
  The secret's *value* is set/rotated out-of-band; Terraform (`terraform/secretsmanager.tf`) only
  manages the secret's existence.
- **`*/package/` directories are CI build output**, not source — CI creates them fresh each run
  (`pip install boto3 --target ./package`, copy the Lambda source in, zip it) and they're gitignored.
  Ignore their contents (including any stale `fetch_reddit.py` / `process_reddit.py` files left over
  from prior local zip runs); the real source is `fetch/fetch_hackernews.py` and
  `process/process_hackernews.py`.
- **`aws_db_instance.main` has `prevent_destroy`** in Terraform — it will hard-error rather than ever
  let `terraform apply`/`destroy` delete the database.
- **Both Lambdas run inside the default VPC** (`terraform/vpc.tf`), in private subnets routed out
  through a single NAT Gateway (needed for the fetch Lambda's calls to the public HN API, and for AWS
  API calls). RDS's security group only allows inbound Postgres from the Lambdas' security group and
  `publicly_accessible = false` — it has no path in from the internet.
