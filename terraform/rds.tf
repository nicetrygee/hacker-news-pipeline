resource "aws_security_group" "rds" {
  name        = "my-pipeline-rds-sg"
  description = "Created by RDS management console"
  vpc_id      = "vpc-37e4d55f"

  # NOTE: this remains open to the internet on purpose for now — the Lambdas
  # aren't in a VPC, so they reach RDS over its public endpoint from AWS's
  # shared, unpredictable IP range. Closing this properly requires putting
  # both Lambdas in the VPC behind a NAT Gateway; tracked as a follow-up.
  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_db_instance" "main" {
  identifier     = "reddit-pipeline-db"
  engine         = "postgres"
  engine_version = "18.3"
  instance_class = "db.t3.micro"

  allocated_storage     = 20
  max_allocated_storage = 1000
  storage_type          = "gp2"

  db_name  = "reddit_pipeline_db"
  username = "pipeline_admin"

  # The real password lives in Secrets Manager (see secretsmanager.tf) and was
  # rotated out-of-band. This value is never applied — ignore_changes below
  # means Terraform will never attempt to reset the live password.
  password = "managed-outside-terraform"

  db_subnet_group_name   = "default-vpc-37e4d55f"
  vpc_security_group_ids = [aws_security_group.rds.id]

  multi_az            = false
  publicly_accessible = true
  storage_encrypted   = true
  kms_key_id          = "arn:aws:kms:eu-west-2:360934290883:key/d7aaec6f-9d25-481d-a30a-203159881937"

  backup_retention_period    = 1
  auto_minor_version_upgrade = true
  copy_tags_to_snapshot      = true
  deletion_protection        = true
  ca_cert_identifier         = "rds-ca-rsa2048-g1"
  network_type               = "IPV4"

  performance_insights_enabled          = true
  performance_insights_kms_key_id       = "arn:aws:kms:eu-west-2:360934290883:key/d7aaec6f-9d25-481d-a30a-203159881937"
  performance_insights_retention_period = 7

  apply_immediately = true

  # prevent_destroy below is the real safety net; skip_final_snapshot only
  # matters if that guard is ever removed, so it's set defensively.
  skip_final_snapshot       = true
  final_snapshot_identifier = "reddit-pipeline-db-final"

  lifecycle {
    prevent_destroy = true
    ignore_changes  = [password, final_snapshot_identifier]
  }
}
