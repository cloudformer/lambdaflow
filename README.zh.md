# lambdaflow —— 真正 serverless 的 Airflow

> **Amazon MWAA** 按环境小时计费，没有 DAG 在跑也照样收钱。**lambdaflow** 用 **Lambda、Step Functions、
> EventBridge Scheduler、DynamoDB 和 S3** 运行同样的 Airflow 3 DAG 文件，没人用时 ≈ $0。
> DAG 文件不用改；规模上来了，把文件拷到 MWAA 就行。

[English](README.md) · [兼容性报告](tests/compat/REPORT.md) · [任务](tests/compat/TASKS.md) · [变更记录](CHANGELOG.md)

![architecture](docs/architecture.svg)

| | Amazon MWAA | MWAA Serverless | lambdaflow |
|---|---|---|---|
| 空闲费用 / 月 | 按环境小时，全天计费（小型环境在 us-east-2 约 $358/月，[AWS](https://aws.amazon.com/blogs/compute/automating-stopping-and-starting-amazon-mwaa-environments-to-reduce-cost/)） | ≈ $0，按 task 分钟 | ≈ $0 |
| DAG 文件 | Python，任意 operator | YAML（DAG factory），Amazon provider 的 operator（[AWS 文档](https://docs.aws.amazon.com/mwaa/latest/mwaa-serverless-userguide/what-is-mwaa-serverless.html)） | Python，Airflow 3（`airflow.sdk`），标准 operator |
| DAG 放在哪 | S3 `dags/` | S3，按版本快照 | S3 `dags/`，上传即解析 |
| 调度器 | Airflow scheduler | AWS 托管 | EventBridge Scheduler，每个 DAG 一个 schedule |
| 界面 | Airflow UI | 无（CloudWatch） | 内置 Airflow 风格控制台（Lambda URL） |
| 元数据 | Aurora（AWS 托管） | AWS 托管 | 一张 DynamoDB 表 |
| Airflow 兼容 | 100% | — | [见报告](tests/compat/REPORT.md) |

## 为什么

多数 Airflow 部署只有几个到几十个 DAG，一天跑几次；其余时间 scheduler、web server、数据库都闲着，MWAA 照样计费。
一次 DAG run 就是一小段突发的工作，正是 Lambda 和 Step Functions 的计费方式擅长的。lambdaflow 保留大家喜欢
Airflow 的部分（Python 写 DAG、`>>`、TaskFlow、trigger rule、重试、XCom、Jinja 模板、Grid 视图），去掉服务器。

## 工作原理

在控制台上传 DAG 文件（或像 MWAA 一样 `aws s3 cp` 到 `dags/`）。**worker** Lambda —— 唯一会导入 DAG 文件的代码 ——
用 lambdaflow 自己的 `airflow` 包解析，把 DAG 存进 **DynamoDB**；不支持的参数当场拒绝，显示为该文件的导入错误。
**console** Lambda 为每个 DAG 建一个 **EventBridge Scheduler** schedule（`0 6 * * 1-5` 变成
`cron(0 6 ? * MON-FRI *)`，时区取 `start_date` 的）。到点触发、或手动触发时，同一个通用 **Step Functions**
状态机的一次执行开始循环：*step*（worker 按 Airflow 的 trigger rule 判断，认领可以跑的 task）→ *run tasks*
（Map，每个 task 调一次 worker）→ *wait*（只在等重试时）→ *step* …… 每次状态变化都是 DynamoDB 条件写入，所以
schedule 触发两次也只建一个 run，task 不会跑两遍。task 日志按 Airflow 的目录格式写到 **S3**。

## 快速开始

### 1. 部署（Terraform）

```bash
cp terraform/example.json terraform/my.json      # aws_profile、env、user、password；task 需要的权限填 task_policy_arns
terraform -chdir=terraform init
terraform -chdir=terraform apply -var-file=my.json
terraform -chdir=terraform output                 # console_url、dags_folder
terraform -chdir=terraform destroy -var-file=my.json   # 全部删除（DAG 文件和日志也会删）
```

apply 时会在本机跑一次 `pip`，打包 Lambda layer（jinja2、tzdata）。CloudFormation：暂未提供。

### 2. 打开控制台

打开 `console_url`，用 `user` / `password` 登录。DAG 列表（暂停开关、schedule、下次运行、最近运行、导入错误）
和每个 DAG 的 **Grid**、**Graph**、**Code**、**Details** 视图都仿照 Airflow UI；点格子看 task 的日志、XCom，
可以 **Clear**、**Mark success / failed**。可以在浏览器里上传或直接写 DAG 文件。命令框接受 airflow CLI 命令：
`dags trigger my_dag -c '{"x": 1}'`、`dags list-runs my_dag`、`tasks states-for-dag-run my_dag <run_id>`（`help` 列出全部）。

### 3. 像写 Airflow 3 一样写 DAG

```python
import datetime
from airflow.sdk import DAG, task
from airflow.providers.standard.operators.bash import BashOperator

with DAG("my_dag", start_date=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
         schedule="0 6 * * *", params={"name": "world"}):

    @task
    def extract():
        return {"rows": 42}

    @task
    def load(data, params=None):
        print(f"loading {data['rows']} rows for {params['name']}")

    load(extract()) >> BashOperator(task_id="report", bash_command="echo 'done for {{ ds }}'")
```

```bash
aws s3 cp my_dag.py "$(terraform -chdir=terraform output -raw dags_folder)"   # 或在控制台上传
```

从 Python 调用（CI、脚本）：`pip install lambdaflow`，然后 `client.connect(console_url, user, password)` ——
`upload`、`trigger`、`wait`、`log`、`clear`、`cli("dags list")`；见 [client/README.md](client/README.md)。

新 DAG 默认暂停，和 Airflow 一样。task 在 AWS 里能访问什么，只能在 Terraform 里授权（`task_policy_arns`），控制台改不了。

### 本地运行，不需要 AWS

```bash
pip install boto3 moto jinja2
python3 src/server/dev.py my_dags/        # http://127.0.0.1:8080，admin / admin；AWS 在内存里模拟
```

同一套控制台、worker 和调度逻辑；状态机换成一个线程，EventBridge 换成一个定时器。

## 兼容性

[tests/compat/REPORT.md](tests/compat/REPORT.md) 每次跑测试都会重新生成：同一批 DAG 文件在真实的 Airflow 3.3.1
（Docker）和 lambdaflow 上各跑一次，比较每个 task 的状态和 try number、每个 XCom 的值；定时则比较 `start_date`
之后的第一次运行时间。报告里还列出每个业务环节由哪些测试覆盖，包括只能部署后验证的部分。

| 状态 | 含义 |
|---|---|
| ✅ verified | 和 Airflow 完全一致，可以放心用 |
| ❌ gap | 已知差异；备注里写了 [TASKS.md](tests/compat/TASKS.md) 中负责修复的任务 |
| ⚪ quirk | 有记录的、刻意保留的差异（例如 `schedule=timedelta`：起点由 EventBridge 决定） |
| ⛔ unsupported | 解析文件时以 `NotSupportedError` 拒绝 —— 绝不悄悄地表现不同 |

目前支持：`DAG` / `@dag` / `@task`（`.branch`、`.short_circuit`）、`>>` / `<<`、PythonOperator、
BranchPythonOperator、ShortCircuitOperator、BashOperator、EmptyOperator、全部 trigger rule、retries 和
retry_delay、XCom（含 `multiple_outputs`）、params 和 `dag_run.conf`、Jinja 模板（`ds`、`ts`、`logical_date`、
`macros.ds_add` ……）、cron / 预设 / `@once` / `timedelta` 定时（含时区）、`max_active_runs`。
不支持 —— 文件解析失败，控制台显示原因：pool、sensor 和 deferrable operator、dynamic task mapping、
dataset / asset 定时、`catchup=True`；task 最长 15 分钟（`task_timeout`）。

```bash
python3 -m pytest tests -q                 # 不碰 Docker：Airflow 在跑就实时比对，否则和记录下来的结果比
python3 -m pytest tests -q --docker up     # 在 Docker 里启动 Airflow 3.3.1，等健康后测试；测完保持运行
python3 -m pytest tests -q --docker run    # 同上，全部通过就关掉；有失败就保持运行并提示
python3 -m pytest tests -q --docker down   # 同上，不管结果一律关掉
```

## 保证

| | 怎么做到 |
|---|---|
| **每个调度点只有一个 run** | run id（`scheduled__<时间>`）是条件写入；重复触发被丢弃 |
| **每次尝试 task 只跑一次** | task 运行前先用条件写入认领；同一个 run 的第二个循环认领不到 |
| **持久性** | 每次状态变化先写 DynamoDB 再进入下一步；task 日志在 S3 |
| **worker 死掉算一次失败尝试** | Lambda 超时或崩溃由状态机捕获，和普通失败一样重试 |
| **最小权限** | 只有 worker 运行你的代码；它只能读 DAG 文件、写日志和自己的表项，外加 `task_policy_arns` |
| **可用性** | Lambda、Step Functions、EventBridge、DynamoDB、S3 都是区域级多可用区服务：没有要打补丁的东西 |

## 适合谁

几十个以内的 DAG，每小时或每天跑，由几分钟内跑完的 Python 和 Bash task 组成 —— ETL 胶水、报表、例行清理。
不适合：超过 15 分钟的 task、要等几个小时的 sensor、几百个并行 task、标准之外的 provider operator。
到这一步就该迁到 MWAA 了 —— 见 [MIGRATION.md](MIGRATION.md)。

## 仓库结构

```
src/server/       lambda_function.py（控制台 + API）、worker.py（运行 DAG 代码）、engine.py（调度逻辑）、
                  schedule.py（cron → EventBridge）、airflow/（Airflow 3 的编写 API）、index.html、dev.py
terraform/        部署
cloudformation/   （暂未提供）
tests/            回归测试；tests/compat/ 兼容性测试、REPORT.md、TASKS.md
```

## 发布

每次 push CI 都跑全部测试。发布就是打 tag：改 `client/pyproject.toml` 和 `client/lambdaflow/__init__.py` 的版本号，
写 CHANGELOG，提交，然后 `git tag vX.Y.Z && git push --tags`（测试 → 构建 → PyPI trusted publishing；tag 必须等于版本号）。

---

**[Redefine AWS Serverless](https://github.com/topics/redefine-aws-serverless)** 系列之一 —— 空闲也计费的 AWS 服务，
用空闲时 ≈ $0 的基础服务重做。MIT。
