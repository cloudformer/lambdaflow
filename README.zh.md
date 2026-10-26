# <project> —— 真正 serverless 的 <replaces>

<!-- 模板:把每个 <占位符> 换掉,章节顺序不要动(这是系列标准,见 SERIES.md) -->

> **<replaces>** 空闲也计费。**<project>** 用 **<built on>** 做同样的事，没人用时 ≈ $0。
> 客户端 API 一样；哪天长大了，改一行 import 就回到真服务。

[English](README.md) · [兼容性报告](tests/compat/REPORT.md) · [任务](tests/compat/TASKS.md) · [更新日志](CHANGELOG.md)

![架构](docs/architecture.svg)

| | <replaces> | <project> |
|---|---|---|
| 空闲费用 / 月 | <idle cost> | ≈ $0 |
| 端点 | <host:port> | Lambda Function URL |
| 存储 | <storage> | <built on> |
| 客户端 | `import <client>` | `from <project> import <client>` |
| 控制台 | <their console> | 自带 web 控制台（Lambda URL） |
| 客户端兼容性 | — | [<n>% / <m> 项](tests/compat/REPORT.md) |

## 为什么

<两三句:真服务空闲花多少钱、这个建在什么上、给谁用>

## 原理

<一段 + 上面的架构图。点名用到的 AWS 原语和各自存什么。>

## 快速开始

### 1. 部署（二选一）

**Terraform**

```bash
cp terraform/example.json terraform/my.json      # 改 aws_profile、env、user、password
terraform -chdir=terraform init
terraform -chdir=terraform apply -var-file=my.json
terraform -chdir=terraform output                 # console_url
terraform -chdir=terraform destroy -var-file=my.json   # 全部拆掉
```

**CloudFormation**（只需要 AWS CLI）

```bash
cp cloudformation/example-params.json cloudformation/params.json
aws cloudformation package --template-file cloudformation/template.yaml \
    --s3-bucket <你自己任意一个桶> --output-template-file /tmp/<project>.yaml
aws cloudformation deploy --template-file /tmp/<project>.yaml --stack-name <project> \
    --capabilities CAPABILITY_IAM --parameter-overrides file://cloudformation/params.json
```

### 2. 打开控制台

浏览器打开 `console_url`，用 `user` / `password` 登录。<控制台能看什么、做什么>。

### 3. 程序连接

```bash
pip install <project>
```

```python
from <project> import <client>        # 以后换真服务:`import <client>`,其他不动

<五行标准客户端 API 的例子>
```

## 兼容性

[tests/compat/REPORT.md](tests/compat/REPORT.md) 每次跑测试自动生成：同样的操作在 Docker 里的真 <replaces>
和 <project> 上各跑一遍，比对结果、类型和错误。

| 状态 | 含义 |
|---|---|
| ✅ verified | 与参照一致，可放心依赖 |
| ❌ gap | 已知差异，备注指向 [TASKS.md](tests/compat/TASKS.md) 里修它的任务 |
| ⚪ quirk | 真服务的怪行为，故意不模仿 |
| ⛔ unsupported | 明确拒绝（`NotSupportedError`），绝不静默走样 |

```bash
docker compose -f tests/compat/docker-compose.yml up -d <service>
<ENV>=<dsn> python3 -m pytest tests -q
docker compose -f tests/compat/docker-compose.yml down
```

## 保证

| | 靠什么 |
|---|---|
| **原子性** | <...> |
| **持久性** | <...> |
| **隔离性** | <...> |
| **可用性** | Lambda / <built on> 是区域级、天然多 AZ 的服务：没有实例会挂、没有维护窗口 |

## 适合谁

<适合的写法、不适合的写法,以及"不适合"就是该迁 <replaces> 的时候——见 MIGRATION.md>

## 目录

```
src/server/       服务本体:lambda_function.py(HTTP API + 控制台)、index.html
client/           pip 包 <project>:<project>/<client>.py、pyproject.toml
terraform/        部署方式 A
cloudformation/   部署方式 B
tests/            回归测试;tests/compat/ 兼容性测试、REPORT.md、TASKS.md
```

## 发版

CI 每次 push 跑全部测试。发版就是打 tag：改 `client/pyproject.toml` 和 `client/<project>/__init__.py` 的版本号，
写 CHANGELOG，提交，然后 `git tag vX.Y.Z && git push --tags`（测试 → 构建 → PyPI trusted publishing；tag 必须等于版本号）。

---

**[Redefine AWS Serverless](https://github.com/topics/redefine-aws-serverless)** 系列之一——把空闲也计费的 AWS 服务，
用空闲 ≈ $0 的原语重做一遍。MIT。
