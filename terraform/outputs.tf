output "console_url" {
  description = "Open in a browser (web console); the same URL is the API endpoint for the client"
  value       = aws_lambda_function_url.server.function_url
}
