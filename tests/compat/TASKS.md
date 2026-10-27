# Tasks

The numbers [REPORT.md](REPORT.md) refers to in its gap notes. Open first; done ones stay below so old
notes keep resolving. Numbers are never reused.

## Open

| task | what |
|---|---|
| 1 | Variables and Connections: `Variable.get` / `{{ var.value.x }}`, `BaseHook.get_connection` / `{{ conn.x }}`; stored in DynamoDB, optional read-only ssm / secretsmanager backend, Airflow's lookup order and JSON import / export; console pages |
| 2 | CloudFormation template producing the same resources as terraform/ |
| 3 | Smoke test after deploy: upload a sample DAG, trigger it, check the run on the real state machine (the "after deploy" row of the test scope) |

Permanent, not tasks: ⚪ quirks (documented behaviour of Airflow we do not imitate: `schedule=timedelta` phase)
and ⛔ unsupported (what needs a running scheduler or workers: pools, sensors / deferrable operators, dynamic
task mapping, asset schedules, catchup, tasks longer than 15 minutes).

## Done

| task | what | release |
|---|---|---|
