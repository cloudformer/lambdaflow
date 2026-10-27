# lambdaflow compatibility report · [tasks](TASKS.md)

Generated 2026-10-26 by `pytest tests` · lambdaflow 0.1.0 · Python 3.9.6

Each row is one feature exercised the same way on the reference and on the project; results, types and errors must match. The feature list only grows (a removed case would show in git), so the support percentage can only move up.

Whole test run: **22 passed**, 0 failed, 0 expected failures (known gaps), 0 skipped — regression suite (`tests/`) and compatibility suite (`tests/compat/`) together.

Open tasks: none — see [TASKS.md](TASKS.md).

## lambdaflow vs Airflow — reference: Apache Airflow 3.3.1 (Docker)

**Support 100%** (8 / 8 features) · 8 verified

✅ verified · ❌ gap — known difference, the note names the task that closes it · ⚪ quirk — documented, deliberately not aligned · ⛔ unsupported by design, refused with NotSupportedError · 🟡 fixed — xfail marker to remove · 🔴 regression · ⏭ not run

| Feature | Status | Note |
|---|---|---|
| **DAGs** | | |
| TaskFlow @dag / @task, return values as arguments, multiple_outputs | ✅ |  |
| a >> b >> c with PythonOperator, XCom pulled downstream | ✅ |  |
| fan-out / fan-in: a >> [b, c] >> d, xcom_pull of a task list | ✅ |  |
| **Execution** | | |
| @task.branch skips the other path; join rules; ShortCircuitOperator skips downstream | ✅ |  |
| every trigger_rule after a failed and a successful upstream; upstream_failed propagation | ✅ |  |
| params overridden by the trigger's conf pick the other branch | ✅ |  |
| retries and retry_delay: succeeds on try 3; exhausts retries and fails | ✅ |  |
| **Templates** | | |
| {{ ds }}, {{ ds_nodash }}, params, dag_run.conf, macros.ds_add, logical_date; Bash stdout as XCom | ✅ |  |
