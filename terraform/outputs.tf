output "fetch_lambda_arn" {
  value = aws_lambda_function.fetch.arn
}

output "process_lambda_arn" {
  value = aws_lambda_function.process.arn
}

output "raw_bucket_name" {
  value = aws_s3_bucket.raw.bucket
}

output "processed_bucket_name" {
  value = aws_s3_bucket.processed.bucket
}

# db_endpoint is temporarily removed along with aws_db_instance.main —
# see the comment at the top of rds.tf.

output "db_secret_arn" {
  value = aws_secretsmanager_secret.db_credentials.arn
}
