# Moving to Amazon MWAA

lambdaflow is for the long tail: a few DAGs, short tasks, idle most of the time, ≈ $0. When you outgrow it,
MWAA runs the same DAG files.

1. Create an MWAA environment on Airflow 3 (3.3 is what lambdaflow is compared with).
2. Copy the DAG files: `aws s3 sync s3://<lambdaflow bucket>/dags/ s3://<mwaa bucket>/dags/` — they use
   `airflow.sdk` and the standard operators, which MWAA has. Nothing in them refers to lambdaflow.
3. Give MWAA's execution role what `task_policy_arns` gave the lambdaflow worker; move `task_env` values to
   Airflow Variables / Connections or to MWAA's configuration.
4. Run history does not move (MWAA keeps its own metadata); unpause the DAGs on MWAA, pause them here.
5. Delete the stack: `terraform -chdir=terraform destroy -var-file=my.json` (this deletes the bucket too).

What you gain: tasks longer than 15 minutes, sensors and deferrable operators, pools, dynamic task mapping,
dataset schedules, catchup, every provider package. What you lose: the ≈ $0 idle bill.
