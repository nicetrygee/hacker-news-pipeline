resource "aws_security_group" "rds" {
  name        = "my-pipeline-rds-sg"
  description = "Created by RDS management console"
  vpc_id      = "vpc-37e4d55f"

  # Both Lambdas now run inside the VPC (see vpc.tf) and reach RDS on its
  # private address, so ingress is scoped to their security group only.
  ingress {
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.lambda.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# aws_db_instance.main is intentionally NOT defined right now.
#
# The live instance was deleted outside Terraform on 2026-08-02 by
# admingregg via the AWS CLI (confirmed via CloudTrail), with a final
# snapshot taken: `reddit-pipeline-db-final-snapshot` (still present,
# state=available, region eu-west-2). Terraform's state still remembers the
# old instance, so leaving a resource block here would make the next
# `terraform apply` recreate it from scratch — empty, with the master
# password set to whatever literal placeholder string is in this file,
# since `ignore_changes` on `password` only suppresses drift *after*
# creation, not the value used to create it.
#
# Left out of scope for this PR (network/security-group scaffolding only).
# Whoever decides to bring the database back needs to explicitly choose,
# and neither is a plain "uncomment this block":
#   - Restore from the snapshot above (add `snapshot_identifier`), to get
#     the old data back.
#   - Or recreate empty, but with a generated password written straight to
#     Secrets Manager — never a literal string in this file.
# Either way, once recreated it should already land inside the Lambda
# security group defined below and with publicly_accessible = false.
