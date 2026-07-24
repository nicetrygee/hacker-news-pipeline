variable "aws_region" {
  description = "AWS region the pipeline runs in"
  type        = string
  default     = "eu-west-2"
}

variable "aws_account_id" {
  description = "AWS account ID the pipeline runs in"
  type        = string
  default     = "360934290883"
}

variable "alert_email" {
  description = "Email address subscribed to the data-quality alerts SNS topic"
  type        = string
  default     = "rosekergregg@gmail.com"
}
