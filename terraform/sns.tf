resource "aws_sns_topic" "alerts" {
  name = "hackernews-pipeline-alerts"
}

resource "aws_sns_topic_subscription" "alerts_email" {
  topic_arn                       = aws_sns_topic.alerts.arn
  protocol                        = "email"
  endpoint                        = var.alert_email
  endpoint_auto_confirms          = false
  confirmation_timeout_in_minutes = 1
}
