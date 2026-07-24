terraform {
  required_version = ">= 1.9"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "s3" {
    bucket         = "hackernews-pipeline-tfstate-360934290883"
    key            = "hackernews-pipeline/terraform.tfstate"
    region         = "eu-west-2"
    dynamodb_table = "hackernews-pipeline-tfstate-lock"
    encrypt        = true
  }
}
