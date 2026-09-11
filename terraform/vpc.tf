# Networking for putting both Lambdas inside the VPC so RDS no longer needs
# to be reachable from the public internet. RDS already lives in the
# account's default VPC (see rds.tf) — reused here rather than standing up a
# separate VPC, since moving an existing `prevent_destroy` RDS instance
# between VPCs would require a snapshot/restore.

data "aws_vpc" "default" {
  id = "vpc-37e4d55f"
}

# The default VPC's own (public) subnets — reused as the NAT Gateway's
# subnet so we don't need to create a new Internet Gateway or public subnet.
data "aws_subnets" "default_public" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }

  filter {
    name   = "default-for-az"
    values = ["true"]
  }
}

data "aws_availability_zones" "available" {
  state = "available"
}

# --- Private subnets for the Lambdas -----------------------------------------
# High indices (200/201 of 256 possible /24s in the default VPC's /16) to
# stay well clear of the low-numbered default public subnets.

resource "aws_subnet" "lambda_private_a" {
  vpc_id            = data.aws_vpc.default.id
  cidr_block        = cidrsubnet(data.aws_vpc.default.cidr_block, 8, 200)
  availability_zone = data.aws_availability_zones.available.names[0]

  tags = {
    Name = "hackernews-pipeline-lambda-private-a"
  }
}

resource "aws_subnet" "lambda_private_b" {
  vpc_id            = data.aws_vpc.default.id
  cidr_block        = cidrsubnet(data.aws_vpc.default.cidr_block, 8, 201)
  availability_zone = data.aws_availability_zones.available.names[1]

  tags = {
    Name = "hackernews-pipeline-lambda-private-b"
  }
}

# --- NAT Gateway --------------------------------------------------------------
# Needed because the fetch Lambda calls out to the public Hacker News API —
# a Lambda ENI in a VPC has no internet access without one, even in a
# "public" subnet. Single NAT Gateway (not one per AZ) to keep cost down for
# this small pipeline; it's a shared dependency for both private subnets.

resource "aws_eip" "nat" {
  domain = "vpc"

  tags = {
    Name = "hackernews-pipeline-nat"
  }
}

resource "aws_nat_gateway" "main" {
  allocation_id = aws_eip.nat.id
  subnet_id     = data.aws_subnets.default_public.ids[0]

  tags = {
    Name = "hackernews-pipeline-nat"
  }
}

# --- Route table for the private subnets --------------------------------------

resource "aws_route_table" "lambda_private" {
  vpc_id = data.aws_vpc.default.id

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.main.id
  }

  tags = {
    Name = "hackernews-pipeline-lambda-private"
  }
}

resource "aws_route_table_association" "lambda_private_a" {
  subnet_id      = aws_subnet.lambda_private_a.id
  route_table_id = aws_route_table.lambda_private.id
}

resource "aws_route_table_association" "lambda_private_b" {
  subnet_id      = aws_subnet.lambda_private_b.id
  route_table_id = aws_route_table.lambda_private.id
}

# --- Security group for the Lambdas -------------------------------------------
# No ingress needed — Lambda doesn't receive inbound traffic. Egress is wide
# open since the functions need HTTPS out to the HN API, S3, SNS, Secrets
# Manager (all via NAT) as well as port 5432 to RDS.

resource "aws_security_group" "lambda" {
  name        = "hackernews-pipeline-lambda-sg"
  description = "Security group for the hackernews-fetch and hackernews-process Lambdas"
  vpc_id      = data.aws_vpc.default.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}
