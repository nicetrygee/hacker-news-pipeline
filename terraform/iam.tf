# --- Lambda execution roles -------------------------------------------------

data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "fetch_lambda" {
  name               = "hackernews-fetch-lambda-role"
  description        = "Execution role for hackernews-fetch Lambda"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

resource "aws_iam_role_policy_attachment" "fetch_lambda_basic_execution" {
  role       = aws_iam_role.fetch_lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "fetch_lambda_permissions" {
  name = "fetch-permissions"
  role = aws_iam_role.fetch_lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "WriteRawStories"
        Effect   = "Allow"
        Action   = "s3:PutObject"
        Resource = "${aws_s3_bucket.raw.arn}/*"
      }
    ]
  })
}

resource "aws_iam_role" "process_lambda" {
  name               = "hackernews-process-lambda-role"
  description        = "Execution role for hackernews-process Lambda"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

resource "aws_iam_role_policy_attachment" "process_lambda_basic_execution" {
  role       = aws_iam_role.process_lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "process_lambda_permissions" {
  name = "process-permissions"
  role = aws_iam_role.process_lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ReadRawStories"
        Effect   = "Allow"
        Action   = "s3:GetObject"
        Resource = "${aws_s3_bucket.raw.arn}/*"
      },
      {
        Sid      = "WriteProcessedCsv"
        Effect   = "Allow"
        Action   = "s3:PutObject"
        Resource = "${aws_s3_bucket.processed.arn}/*"
      },
      {
        Sid      = "PublishDataQualityAlerts"
        Effect   = "Allow"
        Action   = "sns:Publish"
        Resource = aws_sns_topic.alerts.arn
      },
      {
        Sid      = "ReadDbCredentials"
        Effect   = "Allow"
        Action   = "secretsmanager:GetSecretValue"
        Resource = aws_secretsmanager_secret.db_credentials.arn
      }
    ]
  })
}

# --- GitHub Actions OIDC deploy role -----------------------------------------

resource "aws_iam_openid_connect_provider" "github_actions" {
  url             = "https://token.actions.githubusercontent.com"
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = ["ab9d0263244dd0326eb67015705a667e79cfe998"]
}

data "aws_iam_policy_document" "github_actions_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github_actions.arn]
    }
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }
    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["repo:nicetrygee/hacker-news-pipeline:ref:refs/heads/main"]
    }
  }
}

resource "aws_iam_role" "github_actions_deploy" {
  name               = "github-actions-hackernews-pipeline-deploy"
  description        = "Deploy role for nicetrygee/hacker-news-pipeline GitHub Actions workflow (OIDC)"
  assume_role_policy = data.aws_iam_policy_document.github_actions_assume_role.json
}

resource "aws_iam_role_policy" "github_actions_deploy_permissions" {
  name = "lambda-update-function-code"
  role = aws_iam_role.github_actions_deploy.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = "lambda:UpdateFunctionCode"
        Resource = [
          aws_lambda_function.fetch.arn,
          aws_lambda_function.process.arn,
        ]
      }
    ]
  })
}

# --- EventBridge Scheduler invocation role -----------------------------------
# Auto-created by the AWS console when the hourly schedule was first set up.

resource "aws_iam_role" "scheduler_invoke_fetch" {
  name = "Amazon_EventBridge_Scheduler_LAMBDA_c570256325"
  path = "/service-role/"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Action    = "sts:AssumeRole"
        Principal = { Service = "scheduler.amazonaws.com" }
        Condition = {
          StringEquals = { "aws:SourceAccount" = var.aws_account_id }
        }
      }
    ]
  })
}

resource "aws_iam_policy" "scheduler_invoke_fetch" {
  name = "Amazon-EventBridge-Scheduler-Execution-Policy-5e11994d-b241-4843-b619-3c8ea3bb34f6"
  path = "/service-role/"

  # Must reference the current function name (hackernews-fetch) — this policy
  # previously pointed at the pre-rename "reddit-fetch" ARN, which silently
  # broke the hourly schedule's permission to invoke the function for ~2 months.
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["lambda:InvokeFunction"]
        Resource = [
          "${aws_lambda_function.fetch.arn}:*",
          aws_lambda_function.fetch.arn,
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "scheduler_invoke_fetch" {
  role       = aws_iam_role.scheduler_invoke_fetch.name
  policy_arn = aws_iam_policy.scheduler_invoke_fetch.arn
}
