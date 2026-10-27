output "console_url" {
  description = "Open in a browser (web console); the same URL is the JSON API"
  value       = aws_lambda_function_url.console.function_url
}

output "dags_folder" {
  description = "Upload DAG files here as in MWAA (aws s3 cp my_dag.py <dags_folder>), or in the console"
  value       = "s3://${aws_s3_bucket.main.id}/dags/"
}

output "worker_role" {
  description = "The role task code runs as; widen it only with task_policy_arns"
  value       = aws_iam_role.worker.arn
}

output "state_machine" {
  description = "One execution per DAG run (Step Functions console: the run's steps and timings)"
  value       = aws_sfn_state_machine.run.arn
}

output "schedule_group" {
  description = "EventBridge Scheduler group: one schedule per DAG, kept in sync by the console"
  value       = aws_scheduler_schedule_group.dags.name
}
