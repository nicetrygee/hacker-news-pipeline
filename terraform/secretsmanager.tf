# The secret's value (DB host/dbname/username/password/port) is intentionally
# not managed here — Terraform manages the secret's existence, not its
# contents, so the live credential is never written into a .tf file or state
# diff description that a plan/apply log could surface.
resource "aws_secretsmanager_secret" "db_credentials" {
  name                           = "hackernews-pipeline/rds-credentials"
  description                    = "RDS credentials for the hackernews pipeline process Lambda"
  recovery_window_in_days        = 30
  force_overwrite_replica_secret = false
}
