output "console_url" {
  description = "Open in a browser (web console); the same URL is the JSON API"
  value       = module.lambdaflow.console_url
}

output "dags_folder" {
  description = "Upload DAG files here as in MWAA (aws s3 cp my_dag.py <dags_folder>), or in the console"
  value       = module.lambdaflow.dags_folder
}

output "worker_role" {
  description = "The role task code runs as; widen it only with task_policy_arns"
  value       = module.lambdaflow.worker_role
}

output "state_machine" {
  description = "One execution per DAG run (Step Functions console: the run's steps and timings)"
  value       = module.lambdaflow.state_machine
}

output "schedule_group" {
  description = "EventBridge Scheduler group: one schedule per DAG, kept in sync by the console"
  value       = module.lambdaflow.schedule_group
}
