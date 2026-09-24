# automation-orchestrator-python

进程内调度 + 本地持久化的本地任务编排与自动修复执行示例。仅依赖 Python 标准库
与 FastAPI，无外部队列/数据库。

## 快速开始

```bash
uv sync
uv run fastapi dev main.py          # 启动 HTTP 服务（默认并发 2、重试 3 次）
```

提交作业（内置 `healthcheck` 类型，`mode=ok|fatal|flaky`）：

```bash
curl -s -X POST localhost:8000/jobs -H 'content-type: application/json' \
  -d '{"job_id":"j-1","job_type":"healthcheck","payload":{"mode":"flaky","fail_times":2}}'
curl -s localhost:8000/jobs/j-1
```

## 运行测试

```bash
uv run python -m unittest discover -s tests -v
```

## 文档

设计与规则说明（状态模型、并发与维护窗口、重试判定、验证方法）：
[`docs/orchestration.md`](docs/orchestration.md)。

## 代码结构

```
orchestrator/
  states.py     # 作业状态、合法流转表与非法流转原因码
  errors.py     # RetryableJobError / FatalJobError / 非法流转 / 未找到
  models.py     # Job、JobEvent 及 JSON 序列化
  store.py      # JsonJobStore：进程内锁 + 原子落盘
  handlers.py   # 作业处理器注册表
  retry.py      # 失败分类与指数退避
  scheduler.py  # 调度器：FIFO、并发上限、维护窗口 drain、恢复、收敛
main.py         # FastAPI 入口
tests/          # 25 个单元测试
docs/           # 设计文档
```
