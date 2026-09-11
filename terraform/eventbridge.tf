resource "aws_scheduler_schedule" "hourly_fetch" {
  name       = "reddit-pipeline-hourly"
  group_name = "default"

  # Currently disabled in AWS (pinned here so this doesn't get silently
  # re-enabled by an unrelated apply) — pending the RDS decision above,
  # since the process Lambda can't do anything useful without a database.
  state = "DISABLED"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = "rate(1 hours)"
  schedule_expression_timezone = "Europe/London"

  target {
    arn      = aws_lambda_function.fetch.arn
    role_arn = aws_iam_role.scheduler_invoke_fetch.arn

    retry_policy {
      maximum_event_age_in_seconds = 86400
      maximum_retry_attempts       = 0
    }
  }
}
