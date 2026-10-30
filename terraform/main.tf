# lambdaflow: everything is created once here; DAGs, runs and schedules are then managed in the console.
#
#   S3 bucket          dags/ (the DAG files, as in MWAA), logs/ (task logs)
#   DynamoDB table     DAGs, runs, task instances, XComs, import errors
#   Lambda console     Function URL: web console + JSON API; S3 events for dags/; keeps schedules in sync
#   Lambda worker      the only code that imports DAG files and runs tasks (low privilege + task_policy_arns)
#   Lambda layer       jinja2, tzdata
#   Step Functions     ONE generic state machine: start → step → run tasks (Map) → wait → step ...
#   Scheduler group    one EventBridge schedule per DAG, created by the console
#   SQS                dead-letter queue for schedules that could not start a run
#   4 IAM roles        console, worker, state machine, scheduler
# Idle cost: ≈ $0 (no KMS key, no NAT, nothing provisioned).

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  prefix  = "${var.project_name}-${var.env}"
  account = data.aws_caller_identity.current.account_id
  region  = data.aws_region.current.region
  group   = local.prefix
  # the layer's packages, built once by pip for Lambda's Linux / arm64 / Python 3.12
  layer_packages = "jinja2==3.1.6 tzdata==2025.2"
}

# ---------------- storage ----------------
resource "aws_s3_bucket" "main" {
  bucket        = "${local.prefix}-${local.account}"
  force_destroy = true # destroy removes the DAG files and logs too: keep your DAGs in git
}

resource "aws_s3_bucket_public_access_block" "main" {
  bucket                  = aws_s3_bucket.main.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "main" {
  bucket = aws_s3_bucket.main.id
  rule {
    id     = "task-logs"
    status = "Enabled"
    filter { prefix = "logs/" }
    expiration { days = var.task_log_days }
  }
}

resource "aws_dynamodb_table" "main" {
  name         = local.prefix
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"
  range_key    = "sk"
  attribute {
    name = "pk"
    type = "S"
  }
  attribute {
    name = "sk"
    type = "S"
  }
}

resource "aws_sqs_queue" "dlq" {
  name                      = "${local.prefix}-schedule-dlq"
  message_retention_seconds = 1209600
}

# ---------------- code ----------------
data "archive_file" "server" {
  type        = "zip"
  source_dir  = "${path.module}/../src/server"
  excludes    = ["**/__pycache__/**", "**/.DS_Store", "dev.py"]
  output_path = "${path.module}/.build/server.zip"
}

# pip runs once on this machine (and again only when layer_packages changes)
resource "terraform_data" "layer" {
  triggers_replace = local.layer_packages
  provisioner "local-exec" {
    working_dir = path.module
    command     = <<-EOT
      ${var.python} -m pip install --quiet --upgrade --target .build/layer/python --platform manylinux2014_aarch64 \
        --implementation cp --python-version 3.12 --only-binary=:all: ${local.layer_packages} &&
      cd .build/layer && ${var.python} -m zipfile -c ../layer.zip python
    EOT
  }
}

resource "aws_lambda_layer_version" "deps" {
  layer_name               = "${local.prefix}-deps"
  filename                 = "${path.module}/.build/layer.zip"
  source_code_hash         = base64sha256(local.layer_packages)
  compatible_runtimes      = ["python3.12"]
  compatible_architectures = ["arm64"]
  depends_on               = [terraform_data.layer]
}

resource "aws_cloudwatch_log_group" "console" {
  name              = "/aws/lambda/${local.prefix}-console"
  retention_in_days = var.cloudwatch_log_days
}

resource "aws_cloudwatch_log_group" "worker" {
  name              = "/aws/lambda/${local.prefix}-worker"
  retention_in_days = var.cloudwatch_log_days
}

# ---------------- the worker: imports DAG files, runs tasks ----------------
resource "aws_iam_role" "worker" {
  name               = "${local.prefix}-worker"
  assume_role_policy = jsonencode({ Version = "2012-10-17", Statement = [{ Effect = "Allow", Principal = { Service = "lambda.amazonaws.com" }, Action = "sts:AssumeRole" }] })
}

resource "aws_iam_role_policy" "worker" {
  name = "${local.prefix}-worker"
  role = aws_iam_role.worker.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["logs:CreateLogStream", "logs:PutLogEvents"], Resource = "${aws_cloudwatch_log_group.worker.arn}:*" },
      { Effect = "Allow", Action = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:DeleteItem", "dynamodb:Query"],
      Resource = aws_dynamodb_table.main.arn },
      { Effect = "Allow", Action = ["s3:GetObject"], Resource = ["${aws_s3_bucket.main.arn}/dags/*", "${aws_s3_bucket.main.arn}/logs/*"] },
      { Effect = "Allow", Action = ["s3:PutObject"], Resource = "${aws_s3_bucket.main.arn}/logs/*" },
      { Effect = "Allow", Action = ["s3:ListBucket"], Resource = aws_s3_bucket.main.arn }, # a missing log reads as 404, not 403
    ]
  })
}

resource "aws_iam_role_policy_attachment" "tasks" {
  for_each   = toset(var.task_policy_arns)
  role       = aws_iam_role.worker.name
  policy_arn = each.value
}

resource "aws_lambda_function" "worker" {
  function_name    = "${local.prefix}-worker"
  role             = aws_iam_role.worker.arn
  runtime          = "python3.12"
  architectures    = ["arm64"]
  handler          = "worker.lambda_handler"
  filename         = data.archive_file.server.output_path
  source_code_hash = data.archive_file.server.output_base64sha256
  layers           = [aws_lambda_layer_version.deps.arn]
  timeout          = var.task_timeout
  memory_size      = var.worker_memory
  environment {
    variables = merge(var.task_env, {
      TABLE              = aws_dynamodb_table.main.name
      BUCKET             = aws_s3_bucket.main.id
      LAMBDAFLOW_CACHE   = "/tmp/lambdaflow"
      PAUSED_AT_CREATION = var.paused_at_creation ? "1" : "0"
    })
  }
  depends_on = [aws_cloudwatch_log_group.worker]
}

# ---------------- the state machine: one for every DAG run ----------------
locals {
  lambda_retry = [{
    ErrorEquals     = ["Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException", "Lambda.TooManyRequestsException"]
    IntervalSeconds = 2, MaxAttempts = 6, BackoffRate = 2
  }]
  invoke = "arn:aws:states:::lambda:invoke"
  definition = {
    Comment = "lambdaflow: one DAG run. The worker decides everything; this only loops."
    StartAt = "Begin"
    States = {
      Begin = { # {"action": "start"} from EventBridge Scheduler, {"action": "resume"} from the console
        Type       = "Task", Resource = local.invoke, OutputPath = "$.Payload", Retry = local.lambda_retry, Next = "Go"
        Parameters = { FunctionName = aws_lambda_function.worker.arn, "Payload.$" = "$" }
      }
      Go = {
        Type    = "Choice", Default = "Done"
        Choices = [{ Variable = "$.go", BooleanEquals = true, Next = "Step" }]
      }
      Step = {
        Type       = "Task", Resource = local.invoke, OutputPath = "$.Payload", Next = "Route"
        Parameters = { FunctionName = aws_lambda_function.worker.arn, Payload = { action = "step", "dag_id.$" = "$.dag_id", "run_id.$" = "$.run_id" } }
        Retry      = concat(local.lambda_retry, [{ ErrorEquals = ["States.ALL"], IntervalSeconds = 5, MaxAttempts = 3, BackoffRate = 2 }])
      }
      Route = {
        Type    = "Choice", Default = "Wait"
        Choices = [{ Variable = "$.done", IsPresent = true, Next = "Done" }, { Variable = "$.tasks", IsPresent = true, Next = "RunTasks" }]
      }
      RunTasks = {
        Type         = "Map", ItemsPath = "$.tasks", MaxConcurrency = var.max_parallel_tasks, ResultPath = null, Next = "Step"
        ItemSelector = { "dag_id.$" = "$.dag_id", "run_id.$" = "$.run_id", "task_id.$" = "$$.Map.Item.Value" }
        ItemProcessor = {
          ProcessorConfig = { Mode = "INLINE" }
          StartAt         = "RunTask"
          States = {
            RunTask = {
              Type       = "Task", Resource = local.invoke, Retry = local.lambda_retry, End = true
              Parameters = { FunctionName = aws_lambda_function.worker.arn, Payload = { action = "run_task", "dag_id.$" = "$.dag_id", "run_id.$" = "$.run_id", "task_id.$" = "$.task_id" } }
              Catch      = [{ ErrorEquals = ["States.ALL"], ResultPath = "$.error", Next = "TaskFailed" }] # timeout, out of memory
            }
            TaskFailed = {
              Type       = "Task", Resource = local.invoke, Retry = local.lambda_retry, End = true
              Parameters = { FunctionName = aws_lambda_function.worker.arn, Payload = { action = "task_failed", "dag_id.$" = "$.dag_id", "run_id.$" = "$.run_id", "task_id.$" = "$.task_id", "error.$" = "$.error" } }
            }
          }
        }
      }
      Wait = { Type = "Wait", SecondsPath = "$.wait", Next = "Step" }
      Done = { Type = "Succeed" }
    }
  }
}

resource "aws_iam_role" "states" {
  name               = "${local.prefix}-states"
  assume_role_policy = jsonencode({ Version = "2012-10-17", Statement = [{ Effect = "Allow", Principal = { Service = "states.amazonaws.com" }, Action = "sts:AssumeRole", Condition = { StringEquals = { "aws:SourceAccount" = local.account } } }] })
}

resource "aws_iam_role_policy" "states" {
  name = "${local.prefix}-states"
  role = aws_iam_role.states.id
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "lambda:InvokeFunction", Resource = [aws_lambda_function.worker.arn, "${aws_lambda_function.worker.arn}:*"] }]
  })
}

resource "aws_sfn_state_machine" "run" {
  name       = "${local.prefix}-run"
  role_arn   = aws_iam_role.states.arn
  type       = "STANDARD"
  definition = jsonencode(local.definition)
}

# ---------------- EventBridge Scheduler: the group; the schedules are the console's ----------------
resource "aws_scheduler_schedule_group" "dags" {
  name = local.group
}

resource "aws_iam_role" "scheduler" {
  name               = "${local.prefix}-scheduler"
  assume_role_policy = jsonencode({ Version = "2012-10-17", Statement = [{ Effect = "Allow", Principal = { Service = "scheduler.amazonaws.com" }, Action = "sts:AssumeRole", Condition = { StringEquals = { "aws:SourceAccount" = local.account } } }] })
}

resource "aws_iam_role_policy" "scheduler" {
  name = "${local.prefix}-scheduler"
  role = aws_iam_role.scheduler.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = "states:StartExecution", Resource = aws_sfn_state_machine.run.arn },
      { Effect = "Allow", Action = "sqs:SendMessage", Resource = aws_sqs_queue.dlq.arn },
    ]
  })
}

# ---------------- the console: Function URL, S3 events, schedules ----------------
resource "aws_iam_role" "console" {
  name               = "${local.prefix}-console"
  assume_role_policy = jsonencode({ Version = "2012-10-17", Statement = [{ Effect = "Allow", Principal = { Service = "lambda.amazonaws.com" }, Action = "sts:AssumeRole" }] })
}

resource "aws_iam_role_policy" "console" {
  name = "${local.prefix}-console"
  role = aws_iam_role.console.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["logs:CreateLogStream", "logs:PutLogEvents"], Resource = "${aws_cloudwatch_log_group.console.arn}:*" },
      { Effect = "Allow", Action = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:DeleteItem", "dynamodb:Query", "dynamodb:BatchWriteItem"],
      Resource = aws_dynamodb_table.main.arn },
      { Effect = "Allow", Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"], Resource = ["${aws_s3_bucket.main.arn}/dags/*", "${aws_s3_bucket.main.arn}/logs/*"] },
      { Effect = "Allow", Action = ["s3:ListBucket"], Resource = aws_s3_bucket.main.arn },
      { Effect = "Allow", Action = "lambda:InvokeFunction", Resource = aws_lambda_function.worker.arn },
      { Effect = "Allow", Action = "states:StartExecution", Resource = aws_sfn_state_machine.run.arn },
      { Effect = "Allow", Action = ["scheduler:GetSchedule", "scheduler:CreateSchedule", "scheduler:UpdateSchedule", "scheduler:DeleteSchedule"],
      Resource = "arn:aws:scheduler:${local.region}:${local.account}:schedule/${local.group}/*" },
      { Effect = "Allow", Action = "iam:PassRole", Resource = aws_iam_role.scheduler.arn,
      Condition = { StringEquals = { "iam:PassedToService" = "scheduler.amazonaws.com" } } },
    ]
  })
}

resource "aws_lambda_function" "console" {
  function_name    = "${local.prefix}-console"
  role             = aws_iam_role.console.arn
  runtime          = "python3.12"
  architectures    = ["arm64"]
  handler          = "lambda_function.lambda_handler"
  filename         = data.archive_file.server.output_path
  source_code_hash = data.archive_file.server.output_base64sha256
  layers           = [aws_lambda_layer_version.deps.arn]
  timeout          = 60 # an upload waits for the worker to parse the file
  memory_size      = 512
  environment {
    variables = {
      USER_NAME          = var.console_auth_username
      PASSWORD           = var.console_auth_password
      CONSOLE            = var.console ? "1" : "0"
      TABLE              = aws_dynamodb_table.main.name
      BUCKET             = aws_s3_bucket.main.id
      WORKER_FUNCTION    = aws_lambda_function.worker.function_name
      STATE_MACHINE_ARN  = aws_sfn_state_machine.run.arn
      SCHEDULE_GROUP     = aws_scheduler_schedule_group.dags.name
      SCHEDULER_ROLE_ARN = aws_iam_role.scheduler.arn
      DLQ_ARN            = aws_sqs_queue.dlq.arn
      PAUSED_AT_CREATION = var.paused_at_creation ? "1" : "0"
    }
  }
  depends_on = [aws_cloudwatch_log_group.console]
}

# DAG files copied straight to s3://<bucket>/dags/ are parsed too
resource "aws_lambda_permission" "s3" {
  statement_id   = "AllowS3DagEvents"
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.console.function_name
  principal      = "s3.amazonaws.com"
  source_arn     = aws_s3_bucket.main.arn
  source_account = local.account
}

resource "aws_s3_bucket_notification" "dags" {
  bucket = aws_s3_bucket.main.id
  lambda_function {
    lambda_function_arn = aws_lambda_function.console.arn
    events              = ["s3:ObjectCreated:*", "s3:ObjectRemoved:*"]
    filter_prefix       = "dags/"
    filter_suffix       = ".py"
  }
  depends_on = [aws_lambda_permission.s3]
}

# function_url_auth = NONE:    public URL, our user/password is the only gate. A public URL
#                              needs BOTH permission statements (AWS requirement since Oct 2025).
# function_url_auth = AWS_IAM: AWS verifies SigV4 + IAM first; no public permissions are created.
resource "aws_lambda_function_url" "console" {
  function_name      = aws_lambda_function.console.function_name
  authorization_type = var.function_url_auth
  cors {
    allow_origins = ["*"]
    allow_methods = ["*"]
    allow_headers = ["content-type", "authorization"]
  }
}

resource "aws_lambda_permission" "url" {
  count                  = var.function_url_auth == "NONE" ? 1 : 0
  statement_id           = "AllowPublicFunctionUrl"
  action                 = "lambda:InvokeFunctionUrl"
  function_name          = aws_lambda_function.console.function_name
  principal              = "*"
  function_url_auth_type = "NONE"
}

resource "aws_lambda_permission" "url_invoke" {
  count                    = var.function_url_auth == "NONE" ? 1 : 0
  statement_id             = "AllowPublicFunctionUrlInvoke"
  action                   = "lambda:InvokeFunction"
  function_name            = aws_lambda_function.console.function_name
  principal                = "*"
  invoked_via_function_url = true
}
