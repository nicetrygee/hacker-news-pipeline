# Infrastructure

Terraform for the AWS resources behind this pipeline: the two Lambda
functions' configuration, S3 buckets, RDS instance, IAM roles, the SNS alert
topic, the Secrets Manager secret, and the hourly EventBridge Scheduler
trigger.

## What Terraform does and doesn't manage

- **Manages**: every resource's configuration — sizing, IAM permissions,
  bucket settings, RDS parameters, env vars, the schedule expression, etc.
- **Does not manage**: Lambda function *code*. That's still deployed by
  `.github/workflows/deploy.yml` on every push to `main`, exactly as before —
  Terraform's `filename`/`source_code_hash` on both `aws_lambda_function`
  resources are explicitly ignored so a `plan` never conflicts with what CI
  deployed.
- **Does not manage**: the RDS credential *value*. `aws_secretsmanager_secret`
  manages the secret's existence; its contents are set and rotated
  out-of-band so a live password is never written into a `.tf` file, state
  diff, or plan output.

## State

Remote, in S3 (`hackernews-pipeline-tfstate-360934290883`) with DynamoDB
locking (`hackernews-pipeline-tfstate-lock`), both in `eu-west-2`. Neither
bucket nor table is itself Terraform-managed (bootstrapping problem — a
backend can't create the place it stores its own state).

## Usage

Requires AWS credentials with sufficient permissions in account
`360934290883`, region `eu-west-2`.

```
cd terraform
terraform init
terraform plan
terraform apply
```

Applies are **manual only** — this is deliberately not wired into CI. Infra
changes (unlike code deploys) get reviewed before they hit real resources,
especially given `aws_db_instance.main` carries a `prevent_destroy` lifecycle
guard that will hard-error rather than ever let Terraform delete the
database.

## Networking

Both Lambdas run inside the account's default VPC (`vpc.tf`), in two private
subnets routed to the internet through a single NAT Gateway (needed for the
fetch Lambda's calls to the public Hacker News API, and for the AWS API
calls both Lambdas make). RDS (`aws_db_instance.main`) is not publicly
accessible and its security group only allows inbound Postgres traffic from
the Lambdas' security group — it has no path in from the public internet.

## Known gaps

- No automatic secret rotation configured on the Secrets Manager secret.
