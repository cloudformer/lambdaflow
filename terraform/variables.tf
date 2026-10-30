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

variable "project_name" {
  description = "Project name; resources are <project_name>-<env>-console, <project_name>-<env>-worker, ..."
  type        = string
  default     = "lambdaflow"
}

# ---- the Function URL: console, gate, login ----
variable "console" {
  description = "Serve the built-in web console at GET /. false = API only."
  type        = bool
  default     = true
}

variable "function_url_auth" {
  description = <<-EOT
    How the Function URL is protected:
      NONE    = public URL; only console_auth_username/password (HTTP Basic) is checked      <- default
      AWS_IAM = AWS checks a SigV4 signature + IAM permission first. The browser console
                cannot sign requests, so it is unusable in this mode.
  EOT
  type        = string
  default     = "NONE"

  validation {
    condition     = contains(["NONE", "AWS_IAM"], var.function_url_auth)
    error_message = "function_url_auth must be NONE or AWS_IAM."
  }
}

variable "console_auth_username" {
  description = "HTTP Basic user: console login AND every API client; still required with console = false"
  type        = string
}

variable "console_auth_password" {
  description = "HTTP Basic password: console login AND every API client"
  type        = string
  sensitive   = true
}

# ---- lambdaflow ----
variable "task_policy_arns" {
  description = <<-EOT
    IAM policies your tasks need (S3 buckets they read, APIs they call), attached to the worker role.
    Granted here and only here: the console can never widen what task code may do.
  EOT
  type        = list(string)
  default     = []
}

variable "task_env" {
  description = "Environment variables for task code (AIRFLOW_VAR_* / AIRFLOW_CONN_* work as in Airflow)"
  type        = map(string)
  default     = {}
}

variable "task_timeout" {
  description = "Seconds one task may run (the worker Lambda's timeout, at most 900); a task that needs longer belongs on Airflow / MWAA"
  type        = number
  default     = 900

  validation {
    condition     = var.task_timeout >= 10 && var.task_timeout <= 900
    error_message = "task_timeout must be between 10 and 900 seconds (the Lambda limit)."
  }
}

variable "worker_memory" {
  description = "MB for the worker Lambda (CPU grows with it)"
  type        = number
  default     = 1024
}

variable "max_parallel_tasks" {
  description = "Tasks of one DAG run that run at the same time (the state machine's Map concurrency)"
  type        = number
  default     = 10
}

variable "paused_at_creation" {
  description = "New DAGs start paused, as in Airflow"
  type        = bool
  default     = true
}

variable "task_log_days" {
  description = "Days task logs are kept in S3 (logs/)"
  type        = number
  default     = 30
}

variable "cloudwatch_log_days" {
  description = "Days the Lambdas' CloudWatch logs are kept"
  type        = number
  default     = 14
}

variable "python" {
  description = "Local python used once to build the Lambda layer (jinja2, tzdata) with pip"
  type        = string
  default     = "python3"
}
