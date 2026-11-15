# The deployment is the published module: cloudformer/infra/aws//modules/lambdaflow on the Terraform Registry,
# which deploys a released lambdaflow from PyPI (lambdaflow_version, default: the version the module was tested with).
data "aws_caller_identity" "me" {}

locals {
  prefix = "${var.project_name}-${var.env}" # every name the resources already have: <project_name>-<env>-console, ...
}

module "lambdaflow" {
  source  = "cloudformer/infra/aws//modules/lambdaflow"
  version = "0.2.2"
  # source = "../../terraform-aws-infra/modules/lambdaflow" # local: the module's working tree, before a release
  # lambdaflow_version = "X.Y.Z" # unset: the lambdaflow release this module version ships with; set only to deploy another one

  project_name          = local.prefix
  bucket                = "${local.prefix}-${data.aws_caller_identity.me.account_id}" # the existing bucket keeps its DAGs and logs
  console               = var.console
  function_url_auth     = var.function_url_auth
  console_auth_username = var.console_auth_username
  console_auth_password = var.console_auth_password
  task_policy_arns      = var.task_policy_arns
  task_env              = var.task_env
  task_timeout          = var.task_timeout
  worker_memory         = var.worker_memory
  max_parallel_tasks    = var.max_parallel_tasks
  paused_at_creation    = var.paused_at_creation
  task_log_days         = var.task_log_days
  cloudwatch_log_days   = var.cloudwatch_log_days
  force_destroy         = var.force_destroy
}
