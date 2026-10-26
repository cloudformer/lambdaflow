# lambdaflow compatibility report · [tasks](TASKS.md)

Not generated yet: the lambdaflow side of the comparison arrives in phase 2. This file is rewritten by the
first test run in which lambdaflow executes a DAG.

Reference: Apache Airflow 3.3.1 (Docker, `tests/compat/docker-compose.yml`, service `airflow`). What Airflow
does for each feature is already recorded in [expected/](expected/) — one JSON per test: the run's state,
every task's (state, try number) and every XCom value.

| Feature | Status |
|---|---|
| **DAGs** | |
| a >> b >> c with PythonOperator, XCom pulled downstream | ⏭ |
| fan-out / fan-in: a >> [b, c] >> d, xcom_pull of a task list | ⏭ |
| TaskFlow @dag / @task, return values as arguments, multiple_outputs | ⏭ |
| **Execution** | |
| retries and retry_delay: succeeds on try 3; exhausts retries and fails | ⏭ |
| every trigger_rule after a failed and a successful upstream; upstream_failed propagation | ⏭ |
| @task.branch skips the other path; join rules; ShortCircuitOperator skips downstream | ⏭ |
| params overridden by the trigger's conf pick the other branch | ⏭ |
| **Templates** | |
| {{ ds }}, {{ ds_nodash }}, params, dag_run.conf, macros.ds_add, logical_date; Bash stdout as XCom | ⏭ |
