terraform {
  required_version = ">= 1.5"
  required_providers {
    aws     = { source = "hashicorp/aws", version = "~> 6.0" }
    archive = { source = "hashicorp/archive", version = "~> 2.4" }
  }
}

# Credentials, one of (from your -var-file json, see example.json):
#   aws_profile      - a profile in ~/.aws (it may itself carry role_arn)
#   assume_role_arn  - default credentials assume this deploy role
#   both ""          - default credential chain
provider "aws" {
  region  = var.region
  profile = var.aws_profile != "" ? var.aws_profile : null

  dynamic "assume_role" {
    for_each = var.assume_role_arn != "" ? [1] : []
    content {
      role_arn     = var.assume_role_arn
      session_name = "${var.project_name}-deploy"
    }
  }

  default_tags {
    tags = { project = var.project_name, env = var.env }
  }
}
