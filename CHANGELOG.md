# Changelog

## 0.1.0 — 2026-10-27

First release.

- Airflow 3 DAG files (`airflow.sdk`): DAG / @dag / @task (branch, short_circuit), PythonOperator,
  BranchPythonOperator, ShortCircuitOperator, BashOperator, EmptyOperator, all trigger rules, retries,
  XCom, params / conf, Jinja templates; unsupported arguments refused at parse time.
- Schedules: cron, presets, `@once`, `timedelta` → one EventBridge Scheduler schedule per DAG, timezone from
  `start_date`; pause, `max_active_runs`.
- One generic Step Functions state machine per run; conditional writes so a duplicate fire or a second loop
  never runs a task twice; a dead worker counts as a failed try.
- Task logs in S3 (Airflow's layout); Clear (retries start over) and Mark success / failed.
- Built-in Airflow-style console: DAG list, Grid, Graph, Code (edit and save), Details, logs, XCom, airflow
  CLI command box. `src/server/dev.py` runs it all locally on moto.
- `pip install lambdaflow`: a standard-library client for the deployment (upload, trigger, wait, logs, clear, cli).
- Terraform deployment; compatibility report against Airflow 3.3.1 with a test-scope table.
