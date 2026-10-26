# ---- deploy credentials ----
variable "region" {
  type    = string
  default = "us-east-2"
}

variable "aws_profile" {
  description = "Local AWS CLI profile to use; \"\" = default credential chain"
  type        = string
  default     = ""
}

variable "assume_role_arn" {
  description = "Deploy role to assume; \"\" = none. Pick this OR aws_profile."
  type        = string
  default     = ""

  validation {
    condition     = !(var.assume_role_arn != "" && var.aws_profile != "")
    error_message = "Set either aws_profile or assume_role_arn, not both."
  }
}

# ---- app ----
variable "env" {
  description = "Environment label (tags, resource names)"
  type        = string
  default     = "dev"
}

variable "name" {
  description = "Project name; resources are <name>-lambda, <name>-lambda-role"
  type        = string
  default     = "<project>"
}

# ---- the service's credentials (HTTP Basic on the Function URL) ----
variable "user" {
  type = string
}

variable "password" {
  type      = string
  sensitive = true
}

# ---- the endpoint's gate ----
variable "endpoint_auth" {
  description = <<-EOT
    How the Function URL is protected:
      NONE    = public URL; only user/password (HTTP Basic) is checked      <- default
      AWS_IAM = AWS checks a SigV4 signature + IAM permission first. The browser console
                cannot sign requests, so it is unusable in this mode.
  EOT
  type        = string
  default     = "NONE"

  validation {
    condition     = contains(["NONE", "AWS_IAM"], var.endpoint_auth)
    error_message = "endpoint_auth must be NONE or AWS_IAM."
  }
}

variable "console" {
  description = "Serve the built-in web console at GET /. false = API only."
  type        = bool
  default     = true
}
