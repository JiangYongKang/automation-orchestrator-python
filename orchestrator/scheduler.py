"""进程内调度器：并发上限、FIFO 排队、维护窗口、失败重试与终态收敛。"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from typing import Any

from orchestrator.errors import (
    FatalJobError,
    HandlerNotFoundError,
    InvalidStateTransitionError,
    JobNotFoundError,
)
from orchestrator.handlers import HandlerRegistry
from orchestrator.models import Job, JobEvent, SubmitResult
from orchestrator.retry import backoff_delay, classify_error
from orchestrator.states import (
    JobState,
    reject_reason,
)
from orchestrator.store import JsonJobStore

logger = logging.getLogger("orchestrator.scheduler")

# 失败结论码（可追踪）
FR_HANDLER_NOT_FOUND = "handler_not_found"     # 未注册处理器 -> fatal
FR_FATAL_ERROR = "fatal_error"                 # 处理器显式抛出不可重试错误
FR_RETRIES_EXHAUSTED = "retries_exhausted"     # 可重试错误但已达 max_attempts

# 恢复原因
RC_INTERRUPTED_RECOVERY = "interrupted_recovery"


class Scheduler:
    """线程安全的本地作业调度器。

    规则概要（详见 docs/orchestration.md）：
    - 以 ``job_id`` 幂等：重复提交返回既有作业（created=False），不重新入队。
    - 活跃作业数 < ``max_concurrency`` 且非维护窗口时，按 ``submit_seq`` FIFO
      取最早的 PENDING 作业启动。
    - 维护窗口采用 drain 策略：不启动新作业；已在执行的作业不被强杀，
      继续跑到自然终态；若失败需重试则停留 WAITING_RETRY，窗口结束后再启动。
    - 可重试错误 -> WAITING_RETRY + 指数退避，到期重新排队；
      达到 max_attempts -> FAILED 并记录可追踪结论码。
    """

    def __init__(
        self,
        store: JsonJobStore,
        registry: HandlerRegistry,
        *,
        max_concurrency: int = 2,
        default_max_attempts: int = 3,
        base_backoff: float = 0.1,
        backoff_factor: float = 2.0,
        backoff_cap: float = 30.0,
        poll_interval: float = 0.05,
        clock: Callable[[], float] | None = None,
    ) -> None:
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be >= 1")
        self.store = store
        self.registry = registry
        self.max_concurrency = max_concurrency
        self.default_max_attempts = default_max_attempts
        self.base_backoff = base_backoff
        self.backoff_factor = backoff_factor
        self.backoff_cap = backoff_cap
        self.poll_interval = poll_interval
        self._clock = clock or time.time

        self._cond = threading.Condition(threading.RLock())
        self._active = 0
        self._maintenance = False
        self._stopping = False
        self._thread: threading.Thread | None = None
        self._workers: list[threading.Thread] = []
        self._seq = 0

    # ---------------------------------------------------------------- 内部工具
    def _now(self) -> float:
        return self._clock()

    def _record(self, job: Job, kind: str, **kw: Any) -> JobEvent:
        ev = JobEvent(ts=self._now(), kind=kind, **kw)
        job.events.append(ev)
        return ev

    def _log_transition(self, job: Job, src: JobState, dst: JobState,
                        reason: str | None) -> None:
        logger.info(
            "job=%s transition %s -> %s (attempts=%d/%d, reason=%s)",
            job.job_id, src.value, dst.value, job.attempts,
            job.max_attempts, reason,
        )

    def _save(self, job: Job) -> None:
        job.updated_at = self._now()
        self.store.put(job)

    # ---------------------------------------------------------------- 状态流转
    def transition(self, job: Job, to_state: JobState, *,
                   reason: str | None = None,
                   detail: dict[str, Any] | None = None) -> None:
        """校验并执行一次状态流转；非法流转抛 InvalidStateTransitionError。"""
        with self._cond:
            src = job.state
            bad = reject_reason(src, to_state)
            if bad is not None:
                logger.warning(
                    "job=%s rejected transition %s -> %s (%s)",
                    job.job_id, src.value, to_state.value, bad,
                )
                raise InvalidStateTransitionError(
                    job.job_id, src, to_state, bad)
            job.state = to_state
            self._record(
                job, "transition",
                from_state=src.value, to_state=to_state.value,
                attempt=job.attempts, reason=reason, detail=detail or {},
            )
            self._log_transition(job, src, to_state, reason)
            self._save(job)
            self._cond.notify_all()

    # ---------------------------------------------------------------- 提交/查询
    def submit(self, job_id: str, job_type: str,
               payload: dict[str, Any] | None = None,
               *, max_attempts: int | None = None) -> SubmitResult:
        """提交作业。相同 job_id 重复提交幂等返回既有作业，不重新入队。"""
        with self._cond:
            existing = self.store.get(job_id)
            if existing is not None:
                self._record(existing, "duplicate_submit",
                             detail={"job_type": job_type})
                self._save(existing)
                logger.info(
                    "job=%s duplicate submit ignored (state=%s, seq=%d)",
                    job_id, existing.state.value, existing.submit_seq,
                )
                return SubmitResult(job=existing, created=False)

            now = self._now()
            self._seq += 1
            job = Job(
                job_id=job_id,
                job_type=job_type,
                payload=dict(payload or {}),
                state=JobState.PENDING,
                submit_seq=self._seq,
                max_attempts=max_attempts or self.default_max_attempts,
                run_after=now,
                created_at=now,
                updated_at=now,
            )
            self._record(job, "submitted", to_state=JobState.PENDING.value,
                         detail={"job_type": job_type})
            self._save(job)
            logger.info("job=%s submitted (seq=%d, type=%s, max_attempts=%d)",
                        job_id, job.submit_seq, job_type, job.max_attempts)
            self._cond.notify_all()
            return SubmitResult(job=job, created=True)

    def get(self, job_id: str) -> Job:
        with self._cond:
            job = self.store.get(job_id)
            if job is None:
                raise JobNotFoundError(job_id)
            return job

    # ---------------------------------------------------------------- 维护窗口
    def begin_maintenance(self) -> None:
        """进入维护窗口（drain 策略）：不再启动新作业，在执行作业跑到自然终态。"""
        with self._cond:
            self._maintenance = True
            self._cond.notify_all()
        logger.info("maintenance window BEGIN (drain policy, active=%d)",
                    self._active)

    def end_maintenance(self) -> None:
        with self._cond:
            self._maintenance = False
            self._cond.notify_all()
        logger.info("maintenance window END")

    @property
    def in_maintenance(self) -> bool:
        with self._cond:
            return self._maintenance

    @property
    def active_count(self) -> int:
        with self._cond:
            return self._active

    # ---------------------------------------------------------------- 启动恢复
    def recover(self) -> int:
        """启动恢复：上次中断残留的 RUNNING 作业重置为 PENDING 重新调度。

        WAITING_RETRY 作业保持退避不变。返回被恢复的作业数。
        """
        recovered = 0
        with self._cond:
            for job in self.store.all():
                if job.state is JobState.RUNNING:
                    src = job.state
                    job.state = JobState.PENDING
                    self._record(
                        job, "transition",
                        from_state=src.value,
                        to_state=JobState.PENDING.value,
                        attempt=job.attempts,
                        reason=RC_INTERRUPTED_RECOVERY,
                    )
                    self._log_transition(
                        job, src, JobState.PENDING, RC_INTERRUPTED_RECOVERY)
                    self._save(job)
                    recovered += 1
            seqs = [j.submit_seq for j in self.store.all()]
            self._seq = max(seqs, default=0)
        if recovered:
            logger.warning("recovered %d interrupted job(s) -> pending",
                           recovered)
        return recovered

    # ---------------------------------------------------------------- 调度扫描
    def _promote_retry_due(self, now: float) -> None:
        """退避到期的 WAITING_RETRY 作业重新入队（PENDING）。"""
        for job in self.store.all():
            if job.state is JobState.WAITING_RETRY and job.run_after <= now:
                self.transition(job, JobState.PENDING, reason="retry_due")
                self._record(job, "retry_ready", attempt=job.attempts,
                             detail={"run_after": job.run_after})
                self._save(job)

    def run_once(self) -> int:
        """执行一次调度扫描，返回本次新启动的作业数（同步驱动/测试使用）。"""
        started: list[Job] = []
        with self._cond:
            if self._stopping:
                return 0
            now = self._now()
            self._promote_retry_due(now)
            if self._maintenance:
                return 0
            for job in self.store.all():  # 已按 submit_seq 升序
                if self._active >= self.max_concurrency:
                    break
                if job.state is not JobState.PENDING:
                    continue
                if job.run_after > now:
                    continue
                job.state = JobState.RUNNING
                job.attempts += 1
                if job.started_at is None:
                    job.started_at = now
                self._record(job, "started",
                             to_state=JobState.RUNNING.value,
                             attempt=job.attempts)
                self._log_transition(job, JobState.PENDING, JobState.RUNNING,
                                     f"dispatch attempt {job.attempts}")
                self._save(job)
                self._active += 1
                started.append(job)

        # 锁外启动 worker，避免 handler 期间持锁
        for job in started:
            t = threading.Thread(
                target=self._run_job, args=(job.job_id,),
                name=f"job-{job.job_id}", daemon=True,
            )
            t.start()
            with self._cond:
                self._workers.append(t)
        return len(started)

    # ---------------------------------------------------------------- 作业执行
    def _run_job(self, job_id: str) -> None:
        """worker 线程入口：执行 handler 并把作业收敛到终态/重试态。"""
        try:
            with self._cond:
                job = self.store.get(job_id)
                job_type = job.job_type
                payload = dict(job.payload)
                attempt = job.attempts
            try:
                try:
                    handler = self.registry.resolve(job_type)
                except HandlerNotFoundError as exc:
                    # 处理器缺失属于配置错误，重试无意义 -> fatal
                    raise FatalJobError(str(exc),
                                        code=FR_HANDLER_NOT_FOUND) from exc
                result = handler(payload)
                self._on_success(job_id, result)
            except Exception as exc:  # 单个作业失败不得影响调度器
                self._on_failure(job_id, attempt, exc)
        except Exception:  # 兜底：worker 绝不让线程静默死掉
            logger.exception("job=%s worker crashed unexpectedly", job_id)
        finally:
            with self._cond:
                self._active = max(0, self._active - 1)
                self._cond.notify_all()

    def _on_success(self, job_id: str, result: dict[str, Any]) -> None:
        with self._cond:
            job = self.store.get(job_id)
            if job is None or job.state is not JobState.RUNNING:
                return
            job.result = result
            job.finished_at = self._now()
            self._record(job, "succeeded",
                         from_state=JobState.RUNNING.value,
                         to_state=JobState.SUCCEEDED.value,
                         attempt=job.attempts)
            self.transition(job, JobState.SUCCEEDED, reason="handler_ok")
            logger.info("job=%s SUCCEEDED on attempt %d",
                        job_id, job.attempts)

    def _on_failure(self, job_id: str, attempt: int, exc: Exception) -> None:
        error_class = classify_error(exc)
        with self._cond:
            job = self.store.get(job_id)
            if job is None or job.state is not JobState.RUNNING:
                return
            self._record(
                job, "attempt_failed",
                from_state=JobState.RUNNING.value,
                attempt=attempt, error_class=error_class,
                detail={"error_type": type(exc).__name__,
                        "message": str(exc)},
            )
            logger.log(
                logging.WARNING if error_class == "retryable"
                else logging.ERROR,
                "job=%s attempt %d failed error_class=%s type=%s message=%s",
                job_id, attempt, error_class, type(exc).__name__, exc,
            )

            # 不可重试：立即 FAILED
            if error_class == "fatal":
                code = getattr(exc, "code", None) or FR_FATAL_ERROR
                job.failure_reason = code
                job.failure_message = str(exc)
                job.finished_at = self._now()
                self._record(
                    job, "failed",
                    from_state=JobState.RUNNING.value,
                    to_state=JobState.FAILED.value,
                    attempt=attempt, error_class=error_class,
                    reason=code,
                )
                self.transition(job, JobState.FAILED, reason=code)
                logger.error("job=%s FAILED (fatal, attempt %d): %s",
                             job_id, attempt, exc)
                return

            # 可重试但已达上限：FAILED(retries_exhausted)
            if attempt >= job.max_attempts:
                job.failure_reason = FR_RETRIES_EXHAUSTED
                job.failure_message = str(exc)
                job.finished_at = self._now()
                self._record(
                    job, "failed",
                    from_state=JobState.RUNNING.value,
                    to_state=JobState.FAILED.value,
                    attempt=attempt, error_class=error_class,
                    reason=FR_RETRIES_EXHAUSTED,
                )
                self.transition(job, JobState.FAILED,
                                reason=FR_RETRIES_EXHAUSTED)
                logger.error(
                    "job=%s FAILED after %d/%d attempts (retries exhausted): %s",
                    job_id, attempt, job.max_attempts, exc,
                )
                return

            # 可重试且还有额度：WAITING_RETRY + 指数退避
            delay = backoff_delay(
                attempt, self.base_backoff,
                self.backoff_factor, self.backoff_cap,
            )
            job.run_after = self._now() + delay
            self._record(
                job, "retry_scheduled",
                from_state=JobState.RUNNING.value,
                to_state=JobState.WAITING_RETRY.value,
                attempt=attempt, error_class=error_class,
                reason="retryable_error",
                detail={"delay": delay, "next_attempt": attempt + 1},
            )
            self.transition(job, JobState.WAITING_RETRY,
                            reason="retryable_error")
            logger.info(
                "job=%s retry scheduled after %.3fs (attempt %d -> next %d/%d)",
                job_id, delay, attempt, attempt + 1, job.max_attempts,
            )

    # ---------------------------------------------------------------- 生命周期
    def _loop(self) -> None:
        while True:
            with self._cond:
                if self._stopping:
                    return
            self.run_once()
            with self._cond:
                self._cond.wait(timeout=self.poll_interval)

    def start(self) -> None:
        with self._cond:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stopping = False
            self._thread = threading.Thread(
                target=self._loop, name="scheduler", daemon=True)
            self._thread.start()
        logger.info("scheduler started (max_concurrency=%d)",
                    self.max_concurrency)

    def stop(self, wait: bool = True, timeout: float | None = None) -> None:
        """停止调度循环。wait=True 时等待在执行作业自然结束（drain）。"""
        with self._cond:
            self._stopping = True
            self._cond.notify_all()
            thread = self._thread
            workers = list(self._workers)
        if thread is not None:
            thread.join(timeout=timeout)
        if wait:
            deadline = None if timeout is None else time.time() + timeout
            for w in workers:
                remain = None if deadline is None else max(0.0, deadline - time.time())
                w.join(timeout=remain)
        logger.info("scheduler stopped")

    def wait_idle(self, timeout: float | None = None) -> bool:
        """等待直到无运行中/排队/待重试作业。返回是否在超时内达到空闲。"""
        deadline = None if timeout is None else self._now() + timeout
        with self._cond:
            while True:
                pending = [
                    j for j in self.store.all()
                    if j.state in (JobState.PENDING, JobState.RUNNING,
                                   JobState.WAITING_RETRY)
                ]
                if self._active == 0 and not pending:
                    return True
                remain = None if deadline is None else deadline - self._now()
                if remain is not None and remain <= 0:
                    return False
                self._cond.wait(timeout=remain)
