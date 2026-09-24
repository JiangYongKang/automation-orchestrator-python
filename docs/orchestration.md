# 本地任务编排与自动修复执行流程

进程内调度 + 本地 JSON 持久化的运维作业编排器。仅依赖 Python 标准库与仓库已有
依赖（FastAPI），不引入外部队列或数据库。

- 包代码：`orchestrator/`
- HTTP 入口：`main.py`
- 测试：`tests/`

---

## 1. 作业状态模型

| 状态 | 含义 | 是否终态 |
| --- | --- | --- |
| `pending` | 已提交，在 FIFO 队列中等待调度 | 否 |
| `running` | 已被 worker 线程取走执行 | 否 |
| `waiting_retry` | 遇到可重试失败，正在指数退避，到期后重新入队 | 否 |
| `succeeded` | 执行成功 | **是** |
| `failed` | 不可重试失败，或重试次数耗尽 | **是** |

合法流转（实现见 `orchestrator/states.py`）：

```
pending ──调度启动──▶ running ──handler 成功──────────────▶ succeeded
  ▲                     │
  │                     ├── FatalJobError / 处理器缺失────▶ failed
  │                     ├── 可重试且 attempts≥max_attempts▶ failed (retries_exhausted)
  │                     └── 可重试且仍有额度──────────────▶ waiting_retry
  └──────────── 退避到期 (waiting_retry -> pending) ───────┘
```

非法流转一律抛出 `InvalidStateTransitionError`，携带**可区分原因码**：

| reason | 触发情形 |
| --- | --- |
| `terminal_state` | 从终态迁出，例如已完成作业被重新置为 `running` |
| `illegal_transition` | 源状态非终态但不存在该路径，例如 `pending -> succeeded` |
| `already_in_state` | 源状态与目标状态相同 |

另有一个**非常规恢复路径** `running -> pending`，只允许由
`Scheduler.recover()` 在进程启动时执行，事件原因固定为
`interrupted_recovery`，不允许普通 API 触发。

### 幂等与中断恢复

- 提交以 `job_id` 为幂等键：重复提交返回既有作业（`SubmitResult.created=False`），
  不重新入队、不改变 payload 与排队序号，并记录 `duplicate_submit` 事件。
- 持久化采用「写临时文件 + fsync + `os.replace` 原子替换」。
- 进程在作业执行中被中断：重启后 `recover()` 将残留的 `running` 作业重置为
  `pending` 重新调度，已尝试次数 `attempts` 保留，因此重试预算不会被重置。
  `waiting_retry` 作业维持退避时间不变。

每个作业维护完整事件流 `job.events`（submitted / started / transition /
attempt_failed / retry_scheduled / failed / succeeded / duplicate_submit /
retry_ready），可用于解释任意一次状态收敛的来龙去脉。

---

## 2. 并发、排队与维护窗口

### 并发上限与 FIFO 排队

- 构造参数 `max_concurrency`（默认 2）决定同一时刻最多在执行的作业数。
- 调度循环周期性扫描（`poll_interval`，默认 50ms）；当
  `活跃数 < max_concurrency` 时，按提交序号 `submit_seq` 升序取**最早的
  `pending` 作业**启动，因此排队顺序严格等于提交顺序。
- 超过并发上限的作业保持 `pending` 进入队列，**不会被拒绝**。
- 每个作业在独立 daemon 线程执行；共享状态（仓库、计数器、维护标志、
  条件变量）由同一把 `RLock` 保护。

### 维护窗口（drain 策略）

- `begin_maintenance()` 后：调度扫描不再启动任何新作业，队列中的作业保持
  `pending`；退避到期的作业也只停留在队列中。
- **窗口开始时仍在执行的作业采用 drain（排空）处置：不强杀、不打断，让其跑到
  自然终态。** 这是确定且可解释的——
  - 成功则 `succeeded`；
  - 不可重试失败则 `failed`；
  - 可重试失败则进入 `waiting_retry`，**窗口期间不会被重新启动**，
    待 `end_maintenance()` 后按正常退避/排队规则启动。
- `end_maintenance()` 后调度立即恢复，队列中作业继续按 FIFO 执行。

> 选择 drain 而非强杀的原因：运维作业可能持有外部资源或处于不可安全中断的
> 步骤，强杀会留下需要人工判断的半完成状态；drain 保证所有已开始作业都收敛到
> 模型内的确定终态。

### 失败隔离

单个作业的任何异常都在其 worker 线程内被捕获并收敛，调度线程与其它作业不受
影响；调度器不存在「一个作业卡死/炸掉导致整体不可恢复」的路径。

---

## 3. 失败分类与重试退避

判定依据（实现见 `orchestrator/retry.py`、`orchestrator/errors.py`）：

| 异常 | 分类 | 处置 |
| --- | --- | --- |
| `FatalJobError` | `fatal`（不可重试） | 当次即 `failed`，不消耗后续重试 |
| `RetryableJobError` | `retryable` | 退避后重试 |
| 处理器未注册（`HandlerNotFoundError`） | `fatal`（配置错误，重试无意义） | 当次即 `failed`，结论码 `handler_not_found` |
| 其它未分类异常（如 `ValueError`） | `retryable`（保守策略） | 退避后重试，避免偶发故障直接终结作业 |

- 重试预算：`attempts` 在每次启动时 +1；达到 `max_attempts` 后仍遇到可重试
  错误，则终态 `failed`，失败结论码为 **`retries_exhausted`**。
- 退避算法（指数退避，无抖动）：

  ```
  delay(attempt) = min(base_backoff * backoff_factor ** (attempt - 1), backoff_cap)
  ```

  默认 `base=0.1s, factor=2, cap=30s`，序列为 0.1, 0.2, 0.4, … 直到封顶。
- 所有失败结论写入 `job.failure_reason` / `job.failure_message`，并在事件流中
  保留每次尝试的 `error_class`，可追踪最终结论与全部重试过程。

失败结论码汇总：

| `failure_reason` | 含义 |
| --- | --- |
| `fatal_error` | 处理器显式抛出不可重试错误 |
| `handler_not_found` | 作业类型未注册处理器 |
| `retries_exhausted` | 可重试错误但重试预算耗尽 |

---

## 4. 日志可观测性

调度器 logger 名称为 `orchestrator.scheduler`，每条关键日志都打印**作业标识、
状态迁移（from -> to）、尝试次数与失败分类**，例如：

```
job=j2 transition pending -> running (attempts=2/3, reason=dispatch attempt 2)
job=j2 attempt 2 failed error_class=retryable type=RetryableJobError message=transient
job=j2 transition running -> waiting_retry (attempts=2/3, reason=retryable_error)
job=j2 FAILED after 3/3 attempts (retries exhausted): transient
job=j1 rejected transition succeeded -> running (terminal_state)
```

---

## 5. HTTP 接口（main.py）

| 方法与路径 | 说明 |
| --- | --- |
| `GET /` | 服务与调度器状态（并发度、活跃数、维护标志） |
| `POST /jobs` | 提交作业 `{job_id, job_type, payload, max_attempts?}`，响应含 `created` |
| `GET /jobs/{id}` | 查询单个作业（含状态、尝试次数、失败结论、事件流） |
| `GET /jobs` | 按提交顺序列出全部作业 |
| `POST /jobs/{id}/transition` | 显式状态流转；非法流转返回 `409` 及 `reason` |
| `POST /maintenance/begin` `/maintenance/end` | 维护窗口开关 |

环境变量：`ORCHESTRATOR_STORE`（默认 `./data/jobs.json`）、
`ORCHESTRATOR_MAX_CONCURRENCY`（默认 2）、`ORCHESTRATOR_MAX_ATTEMPTS`（默认 3）。

示例（内置 `healthcheck` 作业类型，支持 `mode=ok|fatal|flaky`）：

```bash
curl -s -X POST localhost:8000/jobs -H 'content-type: application/json' \
  -d '{"job_id":"j-1","job_type":"healthcheck","payload":{"mode":"flaky","fail_times":2}}'
curl -s localhost:8000/jobs/j-1
```

启动服务：`uv run fastapi dev main.py`（或 `uv run uvicorn main:app`）。

---

## 6. 验证方法

运行全部单元测试（25 个用例，日志直接打印作业标识、状态迁移与失败分类）：

```bash
uv run python -m unittest discover -s tests -v
```

覆盖矩阵：

| 需求 | 测试文件 / 用例 |
| --- | --- |
| 合法/非法流转、原因码可区分 | `tests/test_states.py` |
| 重复提交幂等、payload/序号不变、duplicate 事件 | `test_submission.py::test_duplicate_submit_is_idempotent` |
| 执行中断后恢复 running -> pending 且次数保留 | `test_submission.py::test_interrupted_running_job_recovers_to_pending` |
| 持久化落盘与重建 | `test_submission.py`、`tests/test_store.py` |
| 超限排队不拒绝 + FIFO 顺序可断言 | `test_concurrency.py::test_over_limit_jobs_queue_fifo_not_rejected` |
| 维护窗口拦截新启动 + drain 在执行作业 + 结束后恢复 | `test_concurrency.py::test_maintenance_window_blocks_new_starts_drains_running` |
| 多线程并发争用不丢作业、序号连续 | `test_concurrency.py::test_concurrent_submissions_do_not_corrupt_state` |
| 重试退避序列、重试后成功、次数可断言 | `test_failures.py` |
| 不可重试立即失败 / 重试耗尽结论码 / 未知异常保守重试 / 处理器缺失 fatal | `test_failures.py` |
| 单作业失败不影响其它作业 | `test_failures.py::test_one_job_failure_does_not_block_others` |

HTTP 层可用 `fastapi.testclient.TestClient` 或启动服务后用 curl 手工验证
（重复提交返回 `created:false`、终态再流转返回 409+`terminal_state`）。
