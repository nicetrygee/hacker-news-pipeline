# Hacker News Pipeline

A small serverless pipeline that pulls the current top 10 Hacker News stories
every hour, validates and transforms them, and lands them in both S3 (as CSV)
and a Postgres database.

## How it works

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

1. **`fetch_hackernews.py`** calls the Hacker News API for the current top 10
   stories and writes the raw JSON to the raw S3 bucket. Triggered hourly by
   an EventBridge Scheduler schedule.
2. That S3 write triggers **`process_hackernews.py`**, which transforms each
   story into a flat record, runs data-quality checks (missing fields,
   invalid scores/ranks, oversized titles), writes an SNS alert for any
   invalid posts, saves the valid posts as a CSV to the processed S3 bucket,
   and inserts them into the `hackernews_posts` table in RDS (deduplicated on
   `post_id` + `fetched_at`).

## Repo layout

```
fetch/fetch_hackernews.py       Lambda: pulls stories from the HN API, writes raw JSON to S3
process/process_hackernews.py   Lambda: validates/transforms, writes CSV + loads RDS, alerts via SNS
tests/                          pytest suite for both Lambdas (mocked AWS/HN calls)
terraform/                      All AWS infrastructure as code (see terraform/README.md)
.github/workflows/deploy.yml    CI: runs tests, then deploys both Lambdas on push to main
```

## Local development

```
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/pytest tests/ -v
```

Tests mock all AWS calls (S3, SNS, Secrets Manager) and the HN API, so no
credentials are needed to run them. `psycopg2` is stubbed automatically in
`tests/conftest.py` if it isn't installed locally — in Lambda it's provided
by a separate layer rather than pip-installed (see
`terraform/lambda.tf`).

## Deployment

Push to `main` triggers `.github/workflows/deploy.yml`:

1. **Run Tests** — the whole suite must pass.
2. **Deploy Fetch Function** / **Deploy Process Function** — zip the
   respective Lambda source and `aws lambda update-function-code`, gated on
   tests passing.

Deploys authenticate to AWS via GitHub's OIDC provider (no long-lived AWS
keys in GitHub) and only touch Lambda *code* — everything else about the
infrastructure is managed separately by Terraform (see below).

## Infrastructure

Everything else — the S3 buckets, RDS instance, IAM roles, the SNS topic, the
Secrets Manager secret holding the DB credentials, and the EventBridge
schedule — is defined in [`terraform/`](terraform/). See
[`terraform/README.md`](terraform/README.md) for usage and the current known
gap (RDS is still reachable on the public internet; closing that needs a VPC
+ NAT Gateway, tracked as a follow-up).
