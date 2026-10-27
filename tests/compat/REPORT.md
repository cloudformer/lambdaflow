# lambdaflow compatibility report · [tasks](TASKS.md)

Generated 2026-10-26 by `pytest tests` · lambdaflow 0.1.0 · Python 3.9.6

Each row is one feature exercised the same way on the reference and on the project; results, types and errors must match. The feature list only grows (a removed case would show in git), so the support percentage can only move up.

Whole test run: **78 passed**, 0 failed, 1 expected failures (known gaps), 0 skipped — regression suite (`tests/`) and compatibility suite (`tests/compat/`) together.

Open tasks: none — see [TASKS.md](TASKS.md).

## Test scope

What each business step is checked by. ✅ every listed test passed · 🔴 one failed · ⏭ some not run (filtered, or an Airflow outcome not recorded yet) · ⏳ still to do.

| Business step | How it is tested | Tests | Status |
|---|---|---|---|
| 1. A DAG file reaches S3 (console upload or `aws s3 cp`) and is parsed | moto S3: unsupported arguments refused (`pool`, `dagrun_timeout`), import errors stored and cleared once fixed, DAGs removed from a file disappear, pause survives a re-parse, S3 events parse / forget | `test_unknown_arguments_are_refused_at_parse_time`<br>`test_import_error_is_stored_and_cleared_when_fixed`<br>`test_a_dag_removed_from_its_file_disappears_and_pause_survives_a_reparse`<br>`test_s3_events_parse_and_forget` | ✅ 4/4 |
| 2. A run is created once; each task runs once | moto DynamoDB, conditional writes: a duplicate trigger returns the same run, a second loop planning the same run cannot claim a task again; the worker drops a duplicate scheduled start | `test_a_run_is_created_once_and_runs_to_the_end`<br>`test_a_task_is_claimed_once_when_two_loops_plan_the_same_run`<br>`test_the_scheduled_start_is_deduplicated_by_the_worker` | ✅ 3/3 |
| 3. Tasks run by dependency: order, fan-out/in, trigger rules, branching, skips, retries | each task's state and try_number compared with Airflow; trigger rules beyond the compat DAG unit-tested | `test_chain`<br>`test_fan_out_in`<br>`test_retries`<br>`test_trigger_rules`<br>`test_branching`<br>`test_branching_conf`<br>`test_trigger_rules_beyond_the_compat_dag` | ✅ 15/15 |
| 4. Data between tasks: XCom, TaskFlow return values, multiple_outputs | XCom values compared with Airflow | `test_chain`<br>`test_fan_out_in`<br>`test_taskflow` | ✅ 3/3 |
| 5. Templates: `{{ ds }}`, params, conf, macros; Bash output | rendered values compared with Airflow | `test_templates`<br>`test_branching_conf` | ✅ 2/2 |
| 6. Scheduling: Airflow schedule → EventBridge Scheduler, pause, max_active_runs | translation and refusals unit-tested; the first run after start_date of 19 schedules compared with Airflow's next_dagrun_run_after; moto Scheduler: the DAG's schedule is created, updated, disabled when paused, deleted with the DAG; a run waits while max_active_runs are running | `test_cron_to_eventbridge`<br>`test_timedelta_to_rate`<br>`test_once_and_timezone_come_from_start_date`<br>`test_refused_with_the_reason`<br>`test_next_fire`<br>`test_next_fire_of_at_and_rate`<br>`test_schedules`<br>`test_schedule_timedelta`<br>`test_schedule_refused`<br>`test_an_unsupported_schedule_is_an_import_error`<br>`test_the_schedule_follows_the_dag`<br>`test_max_active_runs_keeps_a_run_queued` | ✅ 45/45 |
| 7. Task logs, retries, a worker that dies | print / logging / traceback captured to S3 per attempt (Airflow's log path); a timed-out worker is reported by the state machine and retried like any failure | `test_task_logs_retries_and_a_dead_worker` | ✅ 1/1 |
| 8. Console actions: trigger, pause, clear, mark, delete, the airflow CLI box | console Lambda in-process with moto, the state machine replaced by the worker loop; clear gives the task its retries again (Airflow's max_tries) | `test_console_is_public_and_api_needs_auth`<br>`test_upload_trigger_and_read_back_a_run`<br>`test_clear_failed_and_mark`<br>`test_import_errors_unknown_dags_and_delete`<br>`test_pause_unpause`<br>`test_the_airflow_cli_box`<br>`test_clear_reruns_the_task_and_its_downstream_with_the_next_try`<br>`test_mark_success_on_the_failed_leaf_makes_the_run_succeed`<br>`test_delete_dag_removes_runs_logs_and_the_file` | ✅ 9/9 · ⏳ the page itself: by hand in `python3 src/server/dev.py` |
| 9. Variables / Connections; ssm and secretsmanager backends | moto | — | ⏳ later phase |
| 10. Real orchestration: Step Functions loop calling Lambda, EventBridge firing, IAM, real DynamoDB and S3 | not testable locally: `drive()` makes the same `step` / `run_task` calls the state machine makes, `worker.loop()` the same first state; verified on AWS after `terraform apply` | — | ⏳ after deploy |

## lambdaflow vs Airflow — reference: Apache Airflow 3.3.1 (Docker)

**Support 90%** (10 / 11 features) · 1 quirk · 1 unsupported · 9 verified

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
| **Scheduling** | | |
| cron, presets and @once → EventBridge Scheduler: the first run after start_date matches Airflow's, in UTC and named timezones (19 DAGs) | ✅ |  |
| schedule=timedelta(minutes=90) → rate(90 minutes) | ⚪ quirk |  |
| **Unsupported** | | |
| day-of-month and day-of-week both set, a step or L / # in a day field, @continuous, catchup=True | ⛔ |  |
