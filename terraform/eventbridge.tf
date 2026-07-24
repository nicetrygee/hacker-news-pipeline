resource "aws_scheduler_schedule" "hourly_fetch" {
  name       = "reddit-pipeline-hourly"
  group_name = "default"

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
