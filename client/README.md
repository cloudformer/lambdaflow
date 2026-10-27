# lambdaflow

**Python client for [lambdaflow](https://github.com/cloudformer/lambdaflow) — a truly serverless Airflow:
Airflow 3 DAG files on Lambda, Step Functions, EventBridge Scheduler, DynamoDB and S3, ≈ $0 when idle.**
Part of [Redefine AWS Serverless](https://github.com/topics/redefine-aws-serverless).

![lambdaflow architecture](https://raw.githubusercontent.com/cloudformer/lambdaflow/main/docs/architecture.svg)

```bash
pip install lambdaflow
```

```python
from lambdaflow import client

lf = client.connect("https://xxxx.lambda-url.us-east-2.on.aws", "admin", "secret")   # terraform output console_url
lf.upload("my_dag.py")                                   # parsed at once; import errors come back
run_id = lf.trigger("my_dag", conf={"x": 1}, unpause=True)
print(lf.wait("my_dag", run_id))                         # "success" | "failed"
print(lf.log("my_dag", run_id, "extract"))
print(lf.cli("dags list-runs my_dag"))                   # the airflow commands of the console's command box
```

Standard library only. The deployment comes from the [main repository](https://github.com/cloudformer/lambdaflow)
(Terraform). Compatibility with Airflow 3.3:
[REPORT.md](https://github.com/cloudformer/lambdaflow/blob/main/tests/compat/REPORT.md).
