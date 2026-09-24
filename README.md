# automation-orchestrator-python

本地任务编排与自动修复执行流程：进程内调度 + SQLite 本地持久化，模拟运维作业的提交、调度、执行与终态收敛。仅依赖 Python 标准库与仓库已有依赖（FastAPI）。

## 作业状态模型

```
PENDING --> QUEUED --> RUNNING --> SUCCEEDED
                        |   ^
                        |   | (可重试失败，退避后重新入队)
                        v   |
                       QUEUED
                        |
                        +--> FAILED (不可重试 / 达到重试上限)
```

- `SUCCEEDED` / `FAILED` 为终态，任何从终态出发的迁移（如重新置为 RUNNING）都会被拒绝，
  抛出 `InvalidTransitionError`，其 `reason` 字段给出可区分原因
  （`terminal state ...` 或 `transition ... is not allowed`）。
- 状态迁移通过 `compare_and_swap_status` 原子完成：并发争用时只有一个调用方能抢到
  QUEUED→RUNNING 迁移，其余争用者自动退出，保证同一作业不会被重复执行。
- 每次迁移追加到 `job.history`，可通过 `GET /jobs/{job_id}` 追踪完整迁移链。

## 幂等与崩溃恢复

- 提交以客户端提供的 `job_id` 为幂等键：重复提交返回已有作业（`created=false`），不重复入队。
- 服务启动时执行 `recover_interrupted()`：把上次崩溃时停留在 RUNNING 的作业重置为 QUEUED
  重新调度，执行中断后状态仍可收敛。

## 并发与维护窗口规则

- **并发上限**：同时处于 RUNNING 的作业数不超过 `max_concurrency`（默认 2，
  可用环境变量 `ORCH_MAX_CONCURRENCY` 调整）。超出上限的作业进入 FIFO 队列等待，
  不会被拒绝；队列按提交时间（`created_at`）升序派发。
- **维护窗口**：`POST /maintenance/enter` 进入窗口后不再启动新作业；
  窗口开始时仍在执行的作业采用 **drain 语义**——继续运行到自然结束，
  不会被中断或强行置为失败。`POST /maintenance/exit` 退出窗口后恢复派发。

## 失败分类与重试判定

| 异常类型 | 分类 | 处置 |
|---|---|---|
| `RetryableError`、`TimeoutError`、`ConnectionError`、`OSError` | RETRYABLE | 退避后重新入队 |
| `FatalError` 及其它异常 | FATAL | 直接置 FAILED，不重试 |

- 退避间隔：`backoff_base * 2^(attempts-1)`（指数退避），未到退避时间的作业不会被派发。
- 重试上限 `max_retries`（默认 3，按作业可配）：首次执行 + 上限次重试后仍失败，
  置 FAILED 并写入可追踪结论，例如
  `FAILED: retry limit reached (3); last error: RetryableError: ...`。
- 单个作业的失败（包括执行器内的未预期异常）只影响该作业本身，
  调度循环有兜底保护，不会进入不可恢复状态。

## HTTP API

- `POST /jobs` `{job_id, payload, max_retries?}` → `{job_id, status, created}`
- `GET /jobs/{job_id}` → 状态、尝试次数、失败分类、结论、迁移历史
- `POST /maintenance/enter` / `POST /maintenance/exit`

演示处理器规则：payload 为 `retryable` 模拟可重试失败，`fatal` 模拟不可重试失败，其余成功。

## 验证方法

```bash
# 运行全部测试（日志中含作业标识、状态迁移与失败分类）
python3 -m pytest tests/ -q --log-cli-level=INFO

# 启动服务
uvicorn main:app --port 8000
```

测试覆盖（`tests/test_orchestrator.py`）：

- `test_duplicate_submit_is_idempotent`：重复提交幂等，只执行一次
- `test_concurrency_limit_and_fifo_queue`：并发上限、超额排队、FIFO 派发顺序
- `test_retryable_failure_retries_then_succeeds`：可重试失败的重试次数与分类日志
- `test_retry_limit_gives_traceable_conclusion`：达到上限后的可追踪失败结论
- `test_fatal_failure_not_retried`：不可重试失败不重试
- `test_one_failure_does_not_affect_others`：单作业失败不影响其它作业与调度器
- `test_illegal_transition_rejected_with_reason`：非法迁移拒绝及可区分原因
- `test_maintenance_window_blocks_new_starts_and_drains`：维护窗口阻塞新作业、在途作业 drain
- `test_crash_recovery_requeues_interrupted`：崩溃后 RUNNING 作业恢复重跑
