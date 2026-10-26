# State lives next to the code (gitignored). Switch to the S3 backend
# and `terraform init -migrate-state` when more than one person deploys.
terraform {
  backend "local" {
    path = "state/terraform.tfstate"
  }
}
