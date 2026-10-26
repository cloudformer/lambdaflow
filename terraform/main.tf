locals {
  role = "${var.name}-lambda-role"
}

# ---------------- the project's AWS primitive(s) go here ----------------
# e.g. aws_s3_bucket, aws_dynamodb_table, aws_sqs_queue ... and the IAM statements that allow the
# Lambda to use them (add to the policy below).

# ---------------- IAM: one role, least privilege ----------------
resource "aws_iam_role" "lambda" {
  name = local.role
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "lambda" {
  name = "${local.role}-policy"
  role = aws_iam_role.lambda.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "arn:aws:logs:*:*:*"
      },
      # { Effect = "Allow", Action = ["dynamodb:GetItem", ...], Resource = aws_dynamodb_table.x.arn },
    ]
  })
}

# ---------------- the service: one Lambda + Function URL (console + API) ----------------
data "archive_file" "server" {
  type        = "zip"
  source_dir  = "${path.module}/../src/server"
  excludes    = ["**/__pycache__/**", "**/.DS_Store"]
  output_path = "${path.module}/.build/server.zip"
}

resource "aws_lambda_function" "server" {
  function_name    = "${var.name}-lambda"
  role             = aws_iam_role.lambda.arn
  runtime          = "python3.12"
  handler          = "lambda_function.lambda_handler"
  filename         = data.archive_file.server.output_path
  source_code_hash = data.archive_file.server.output_base64sha256
  timeout          = 30
  memory_size      = 256

  environment {
    variables = {
      USER_NAME = var.user
      PASSWORD  = var.password
      CONSOLE   = var.console ? "1" : "0"
    }
  }
}

# endpoint_auth = NONE:    public URL, our user/password is the only gate. A public URL
#                          needs BOTH permission statements (AWS requirement since Oct 2025).
# endpoint_auth = AWS_IAM: AWS verifies SigV4 + IAM first; no public permissions are created.
resource "aws_lambda_function_url" "server" {
  function_name      = aws_lambda_function.server.function_name
  authorization_type = var.endpoint_auth
  cors {
    allow_origins = ["*"]
    allow_methods = ["*"]
    allow_headers = ["content-type", "authorization"]
  }
}

resource "aws_lambda_permission" "url" {
  count                  = var.endpoint_auth == "NONE" ? 1 : 0
  statement_id           = "AllowPublicFunctionUrl"
  action                 = "lambda:InvokeFunctionUrl"
  function_name          = aws_lambda_function.server.function_name
  principal              = "*"
  function_url_auth_type = "NONE"
}

resource "aws_lambda_permission" "url_invoke" {
  count                    = var.endpoint_auth == "NONE" ? 1 : 0
  statement_id             = "AllowPublicFunctionUrlInvoke"
  action                   = "lambda:InvokeFunction"
  function_name            = aws_lambda_function.server.function_name
  principal                = "*"
  invoked_via_function_url = true
}
