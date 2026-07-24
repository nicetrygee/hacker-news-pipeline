# Terraform manages the Lambda functions' configuration (role, env vars, sizing),
# not their code — that's deployed independently by the GitHub Actions workflow
# on every push to main. filename/source_code_hash are ignored so plans never
# show a diff or attempt to overwrite what CI deployed.

resource "aws_lambda_function" "fetch" {
  function_name = "hackernews-fetch"
  role          = aws_iam_role.fetch_lambda.arn
  handler       = "fetch_hackernews.lambda_handler"
  runtime       = "python3.13"
  architectures = ["x86_64"]
  timeout       = 30
  memory_size   = 128

  filename         = "${path.module}/placeholder-lambda.zip"
  source_code_hash = filebase64sha256("${path.module}/placeholder-lambda.zip")

  lifecycle {
    ignore_changes = [filename, source_code_hash]
  }
}

resource "aws_lambda_function" "process" {
  function_name = "hackernews-process"
  role          = aws_iam_role.process_lambda.arn
  handler       = "process_hackernews.lambda_handler"
  runtime       = "python3.13"
  architectures = ["x86_64"]
  timeout       = 30
  memory_size   = 128
  publish       = false

  # psycopg2 isn't pip-installed by the deploy workflow (see .github/workflows/deploy.yml) —
  # it's provided at runtime by this pre-built layer instead.
  layers = ["arn:aws:lambda:eu-west-2:360934290883:layer:psycopg2-layer:1"]

  filename         = "${path.module}/placeholder-lambda.zip"
  source_code_hash = filebase64sha256("${path.module}/placeholder-lambda.zip")

  environment {
    variables = {
      SNS_TOPIC_ARN = aws_sns_topic.alerts.arn
      DB_SECRET_ARN = aws_secretsmanager_secret.db_credentials.arn
    }
  }

  lifecycle {
    ignore_changes = [filename, source_code_hash]
  }
}

resource "aws_lambda_permission" "allow_s3_invoke_process" {
  statement_id   = "lambda-321f9bb7-d8d0-4943-8001-61d165c6cbae"
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.process.function_name
  principal      = "s3.amazonaws.com"
  source_arn     = aws_s3_bucket.raw.arn
  source_account = var.aws_account_id
}
