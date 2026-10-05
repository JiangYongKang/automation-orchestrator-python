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
  不会被拒绝。
- **维护窗口**：`POST /maintenance/enter` 进入窗口后不再启动新作业（任何优先级都不例外）；
  窗口开始时仍在执行的作业采用 **drain 语义**——继续运行到自然结束，
  不会被中断或强行置为失败。`POST /maintenance/exit` 退出窗口后恢复派发。

## 优先级调度与防饿死

- **取值**：提交时可带 `priority`，接受命名级别 `high`(10) / `normal`(0) / `low`(-10)
  （大小写不敏感）或任意整数（数值越大越优先），缺省为 `normal`。
  非法取值（无法解析的字符串、布尔值）被拒绝并返回 422。
- **派发顺序**：每轮调度按“有效优先级降序”取作业，同一有效优先级内部按入队时间
  先来先服务（FIFO），平局再按 job_id 保证确定性。全部取默认优先级时行为与
  引入优先级之前完全一致（纯 FIFO）。
- **防饿死（aging）**：有效优先级 = 静态优先级 + `aging_step * floor(等待时长 / aging_interval)`，
  等待时长从最近一次入队时刻（`queued_at`）起算。默认 `aging_interval=1.0s`、
  `aging_step=1`（可用环境变量 `ORCH_AGING_INTERVAL` / `ORCH_AGING_STEP` 调整）。
  由于有效优先级随等待时间无界增长，只要**新到作业的静态优先级有上界**
  （命名级别天然满足，最高为 high=10），任何作业等待
  `(上界 - 自身优先级) / aging_step * aging_interval` 后必然反超新到作业，
  不会被无限期卡在队列里。边界情况：若调用方持续提交数值无上限的自定义优先级，
  该保证不成立——需要硬保证时请只使用命名级别。
- **与既有规则的兼容**：优先级只在“有并发余量且不在维护窗口内”时决定谁先派发，
  不能突破并发上限，也不能在维护窗口期间插队启动；退避中（`not_before` 未到）
  的作业本轮不参与派发，期间其它就绪作业（含更低优先级）可正常被调度。
- **重试后的优先级**：可重试失败退避后重新入队时**保留原静态优先级**，
  老化计时从重新入队时刻重新开始（`queued_at` 重置），退避到期后按原优先级
  参与排序——既不会让重试作业因历史等待时间插队到最前，也不会被降权。
- **幂等去重不受影响**：同 `job_id` 重复提交仍返回首次提交的作业，
  优先级以首次提交为准，不会被后续提交覆盖。

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
- `GET /jobs/{job_id}` → 状态、尝试次数、优先级、失败分类、结论、迁移历史
- `POST /maintenance/enter` / `POST /maintenance/exit`

演示处理器规则：payload 为 `retryable` 模拟可重试失败，`fatal` 模拟不可重试失败，其余成功。

## 验证方法

```bash
# 运行全部测试（日志中含作业标识、状态迁移与失败分类）
python3 -m pytest tests/ -q --log-cli-level=INFO

# 启动服务
uvicorn main:app --port 8000
```

本地验证优先级行为（服务启动后）：

```bash
# 进入维护窗口，让作业先积压在队列里
curl -X POST localhost:8000/maintenance/enter
curl -X POST localhost:8000/jobs -H 'Content-Type: application/json' \
  -d '{"job_id": "batch-1", "payload": "ok", "priority": "low"}'
curl -X POST localhost:8000/jobs -H 'Content-Type: application/json' \
  -d '{"job_id": "alert-1", "payload": "ok", "priority": "high"}'
# 退出窗口后 alert-1 先于 batch-1 被派发（history 中可见迁移时刻）
curl -X POST localhost:8000/maintenance/exit
curl localhost:8000/jobs/alert-1
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

优先级调度覆盖（`tests/test_priority.py`）：

- `test_normalize_priority_named_and_numeric`：命名级别 / 数值优先级解析与非法值拒绝
- `test_effective_priority_aging`：有效优先级随等待时间增长，低优先级最终反超
- `test_high_priority_dispatched_before_lower`：不同优先级之间的派发顺序
- `test_same_priority_fifo_preserved`：同优先级内部先来先服务
- `test_numeric_priority_ordering`：自定义数值优先级按大小派发
- `test_low_priority_not_starved_by_continuous_high_influx`：高优先级持续涌入下低优先级靠老化最终被调度
- `test_priority_respects_concurrency_limit`：优先级不突破并发上限
- `test_maintenance_window_blocks_high_priority_too`：维护窗口内高优先级同样被阻塞
- `test_retry_keeps_priority_and_requeues_ahead_of_low`：重试保留优先级、退避到期后按原优先级调度
- `test_duplicate_submit_with_priority_is_idempotent`：带优先级的重复提交幂等、保留首次优先级
