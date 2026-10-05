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
  可用环境变量 `ORCH_MAX_CONCURRENCY` 调整）。超出上限的作业进入队列等待，
  不会被拒绝；**优先级不能突破并发上限**。
- **维护窗口**：`POST /maintenance/enter` 进入窗口后不再启动新作业（任何优先级都不例外）；
  窗口开始时仍在执行的作业采用 **drain 语义**——继续运行到自然结束，
  不会被中断或强行置为失败。`POST /maintenance/exit` 退出窗口后恢复派发。

## 优先级调度与防饿死

### 优先级取值

提交时可带 `priority` 字段，缺省为 `normal`：

| 取值 | 归一化数值 | 说明 |
|---|---|---|
| `"high"` | 0 | 线上告警等紧急作业 |
| `"normal"` | 5 | 默认值 |
| `"low"` | 10 | 离线跑批等可延后作业 |
| 非负整数 | 原值（负值按 0 处理） | 自定义档位，**数值越小越优先** |

非法取值（未知名称、布尔等）被拒绝：`submit()` 抛 `ValueError`，HTTP 层返回 422。

### 排队与派发规则

- 派发顺序按 **(有效优先级, 入队时间 `queued_at`, job_id)** 排序：有效优先级高的先派发；
  **同一优先级内部严格先来先服务**（按入队时间），引入优先级不会打乱同级顺序。
- 排序只决定"有空闲并发额度时先派谁"，不影响并发上限、维护窗口、退避等其它规则。

### 防饿死（aging）规则

- 每个作业记录最近一次入队时刻 `queued_at`。等待时长 = 当前时间 − `queued_at`。
- 等待超过 `aging_threshold` 秒（默认 30，可用环境变量 `ORCH_AGING_THRESHOLD` 调整）
  的作业被**提升到插队档**（有效优先级 −1，高于一切普通/自定义档位）；
  插队档内部仍按 `queued_at` 先来先服务。
- **可判定性**：等待时长单调增长，任何作业最终必然进入插队档；一旦进入，
  晚入队的作业（`queued_at` 更晚）永远无法越过它，而在它之前的作业数量是有限的。
  只要系统在正常运转（作业会执行完、并发额度会释放），任何作业最终都能被派发，
  不会无限期卡在队列里。
- 边界情况：
  - `aging_threshold` 必须 ≥ 0；阈值越小提升越快，为 0 时等价于纯 FIFO。
  - 退避中的作业（`not_before` 未到）即使被提升也不会提前启动，退避规则优先。
  - 维护窗口内不派发任何作业，等待时长照常累计，窗口结束后按上述规则派发。

### 重试与优先级的交互

- 可重试失败退避后重新入队时：**保留原优先级**，`queued_at` 重置为重新入队时刻
  （防饿死计时重新起算），`not_before` 退避照常生效。
- 崩溃恢复（`recover_interrupted`）把 RUNNING 重置为 QUEUED 时同样重置 `queued_at`：
  崩溃期间系统不在运转，崩溃前的等待时长不计入。
- 因此重试/恢复的作业回到同级队尾，既不会长期霸占插队档，也不会搅乱同级 FIFO。

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

- `POST /jobs` `{job_id, payload, max_retries?, priority?}` → `{job_id, status, priority, created}`
  （`priority` 非法时返回 422）
- `GET /jobs/{job_id}` → 状态、优先级、尝试次数、失败分类、结论、迁移历史
- `POST /maintenance/enter` / `POST /maintenance/exit`

演示处理器规则：payload 为 `retryable` 模拟可重试失败，`fatal` 模拟不可重试失败，其余成功。

## 验证方法

```bash
# 运行全部测试（日志中含作业标识、状态迁移与失败分类）
python3 -m pytest tests/ -q --log-cli-level=INFO

# 启动服务
uvicorn main:app --port 8000
```

本地验证优先级行为（另开终端）：

```bash
# 占满并发（默认 2），再按 低->高 顺序提交，观察派发顺序
curl -X POST localhost:8000/jobs -H 'content-type: application/json' -d '{"job_id":"a1","payload":"retryable","priority":"low"}'
curl -X POST localhost:8000/jobs -H 'content-type: application/json' -d '{"job_id":"a2","payload":"retryable","priority":"low"}'
curl -X POST localhost:8000/jobs -H 'content-type: application/json' -d '{"job_id":"b1","payload":"ok","priority":"low"}'
curl -X POST localhost:8000/jobs -H 'content-type: application/json' -d '{"job_id":"b2","payload":"ok","priority":"high"}'
curl localhost:8000/jobs/b2   # 高优先级的 b2 先于 b1 进入 RUNNING
# 防饿死：调小阈值观察低优先级被提升
ORCH_AGING_THRESHOLD=5 uvicorn main:app --port 8000
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
- `test_priority_dispatch_order`：不同优先级的派发顺序，且优先级不突破并发上限
- `test_same_priority_is_fifo`：同优先级内部先来先服务
- `test_low_priority_eventually_dispatched_under_high_pressure`：高优先级持续涌入时低优先级最终被提升并派发（防饿死）
- `test_maintenance_window_blocks_high_priority_too`：维护窗口内高优先级也不能插队
- `test_retried_job_keeps_priority_and_requeues_fresh`：重试保留优先级、queued_at 重置
- `test_invalid_priority_rejected`：非法优先级取值被拒绝且不创建作业
- `test_duplicate_submit_keeps_original_priority`：重复提交去重后保留首次提交的优先级
