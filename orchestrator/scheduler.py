"""进程内调度器：并发上限、FIFO 排队、维护窗口、重试退避。"""
from __future__ import annotations

import logging
import threading
import time

from .executor import JobHandler, execute
from .models import (
    FailureClass,
    JobStatus,
    effective_priority,
    validate_transition,
)
from .store import JobStore

logger = logging.getLogger("orchestrator.scheduler")


class Scheduler:
    """后台线程驱动的调度循环。

    - 并发上限：同时在 RUNNING 的作业数不超过 max_concurrency，超出部分留在队列。
    - 优先级调度：有效优先级高的先派发，同优先级内按入队时间（queued_at）先来先服务。
    - 防饿死：作业等待超过 aging_threshold 秒即被提升到插队档（见 models.effective_priority）。
    - 维护窗口：窗口内不启动新作业；窗口开始时仍在执行的作业继续跑完（drain 语义）。
    - 重试退避：可重试失败按 backoff_base * 2^(attempts-1) 退避后重新入队；
      重新入队保留原优先级，queued_at 重置为重新入队时刻。
    """

    def __init__(
        self,
        store: JobStore,
        handler: JobHandler,
        max_concurrency: int = 2,
        backoff_base: float = 0.1,
        tick_interval: float = 0.02,
        aging_threshold: float = 30.0,
    ) -> None:
        self._store = store
        self._handler = handler
        self._max_concurrency = max_concurrency
        self._backoff_base = backoff_base
        self._tick_interval = tick_interval
        self._aging_threshold = aging_threshold
        self._maintenance = threading.Event()
        self._stop = threading.Event()
        self._cond = threading.Condition()
        self._running = 0
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """启动调度线程（崩溃恢复由调用方在 start 前完成）。"""
        self._thread = threading.Thread(target=self._loop, name="scheduler", daemon=True)
        self._thread.start()

    def stop(self, drain: bool = True) -> None:
        """停止调度；drain=True 时等待在途作业结束后再返回。"""
        self._stop.set()
        with self._cond:
            self._cond.notify_all()
        if self._thread:
            self._thread.join(timeout=5)
        if drain:
            self.wait_idle(timeout=5)

    def wait_idle(self, timeout: float = 5.0) -> bool:
        """等待没有在途作业且队列为空，返回是否收敛。"""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self._cond:
                running = self._running
            if running == 0 and not self._store.list_by_status(JobStatus.QUEUED):
                return True
            time.sleep(self._tick_interval)
        return False

    def enter_maintenance(self) -> None:
        """进入维护窗口：不再启动新作业，在途作业继续跑完（drain 语义）。"""
        self._maintenance.set()
        logger.info("maintenance window entered: draining in-flight jobs")

    def exit_maintenance(self) -> None:
        """退出维护窗口，恢复调度。"""
        self._maintenance.clear()
        with self._cond:
            self._cond.notify_all()
        logger.info("maintenance window exited")

    @property
    def in_maintenance(self) -> bool:
        return self._maintenance.is_set()

    def running_count(self) -> int:
        """当前 RUNNING 作业数。"""
        with self._cond:
            return self._running

    def notify(self) -> None:
        """唤醒调度循环（新作业入队时调用）。"""
        with self._cond:
            self._cond.notify_all()

    # ---- 内部实现 ----

    def _loop(self) -> None:
        while not self._stop.is_set():
            self._tick()
            with self._cond:
                self._cond.wait(timeout=self._tick_interval)

    def _tick(self) -> None:
        """单轮调度：在并发余量内按 (有效优先级, 入队时间) 取作业并派发到工作线程。"""
        if self._maintenance.is_set():
            return
        now = time.time()
        with self._cond:
            capacity = self._max_concurrency - self._running
        if capacity <= 0:
            return
        queued = self._store.list_by_status(JobStatus.QUEUED)
        # 有效优先级（含防饿死提升）高的在前；同级按入队时间 FIFO，job_id 兜底保证稳定
        queued.sort(key=lambda j: (effective_priority(j, now, self._aging_threshold),
                                   j.queued_at, j.job_id))
        for job in queued:
            if capacity <= 0:
                break
            if job.not_before > now:
                continue  # 退避中，未到可调度时间
            validate_transition(job.job_id, JobStatus.QUEUED, JobStatus.RUNNING)
            claimed = self._store.compare_and_swap_status(
                job.job_id, JobStatus.QUEUED, JobStatus.RUNNING,
                attempts=job.attempts + 1, updated_at=now,
                history=job.history + [f"QUEUED->RUNNING attempt={job.attempts + 1}"],
            )
            if not claimed:
                continue  # 并发争用失败，其它线程已接管
            with self._cond:
                self._running += 1
                capacity -= 1
            logger.info("job=%s transition QUEUED->RUNNING attempt=%d",
                        job.job_id, job.attempts + 1)
            threading.Thread(
                target=self._run_job, args=(job.job_id,), daemon=True,
                name=f"job-{job.job_id}",
            ).start()

    def _run_job(self, job_id: str) -> None:
        """执行单个作业并处理重试 / 终态收敛，异常不外溢。"""
        try:
            job = self._store.get(job_id)
            assert job is not None
            result = execute(self._handler, job.payload)
            now = time.time()
            fresh = self._store.get(job_id)
            assert fresh is not None
            if result.succeeded:
                self._store.compare_and_swap_status(
                    job_id, JobStatus.RUNNING, JobStatus.SUCCEEDED,
                    updated_at=now, conclusion="ok",
                    history=fresh.history + ["RUNNING->SUCCEEDED"],
                )
                logger.info("job=%s transition RUNNING->SUCCEEDED", job_id)
            elif result.failure_class == FailureClass.RETRYABLE and fresh.attempts <= fresh.max_retries:
                delay = self._backoff_base * (2 ** (fresh.attempts - 1))
                # 重新入队：保留原优先级，queued_at 重置（防饿死计时重新起算），退避照常生效
                self._store.compare_and_swap_status(
                    job_id, JobStatus.RUNNING, JobStatus.QUEUED,
                    updated_at=now, queued_at=now, not_before=now + delay,
                    failure_class=result.failure_class.value,
                    history=fresh.history + [
                        f"RUNNING->QUEUED retry attempt={fresh.attempts} "
                        f"class=RETRYABLE backoff={delay:.3f}s detail={result.detail}"
                    ],
                )
                logger.info(
                    "job=%s retry attempt=%d class=RETRYABLE backoff=%.3fs detail=%s",
                    job_id, fresh.attempts, delay, result.detail,
                )
            else:
                reason = (
                    "non-retryable failure"
                    if result.failure_class == FailureClass.FATAL
                    else f"retry limit reached ({fresh.max_retries})"
                )
                conclusion = f"FAILED: {reason}; last error: {result.detail}"
                self._store.compare_and_swap_status(
                    job_id, JobStatus.RUNNING, JobStatus.FAILED,
                    updated_at=now, failure_class=result.failure_class.value,
                    conclusion=conclusion,
                    history=fresh.history + [f"RUNNING->FAILED {reason}"],
                )
                logger.info(
                    "job=%s transition RUNNING->FAILED class=%s reason=%s",
                    job_id, result.failure_class.value if result.failure_class else "UNKNOWN",
                    reason,
                )
        except Exception:  # noqa: BLE001 - 单作业异常不得拖垮调度器
            logger.exception("job=%s unexpected error in runner, scheduler continues", job_id)
        finally:
            with self._cond:
                self._running -= 1
                self._cond.notify_all()
