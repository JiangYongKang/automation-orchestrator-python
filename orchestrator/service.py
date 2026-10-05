"""编排服务门面：提交（幂等）、查询、生命周期与维护窗口控制。"""
from __future__ import annotations

import logging
import time

from .executor import JobHandler
from .models import Job, JobStatus, normalize_priority, validate_transition
from .scheduler import Scheduler
from .store import JobStore

logger = logging.getLogger("orchestrator.service")


class OrchestratorService:
    """对外暴露的编排入口。"""

    def __init__(
        self,
        handler: JobHandler,
        db_path: str = ":memory:",
        max_concurrency: int = 2,
        max_retries: int = 3,
        aging_interval: float = 1.0,
        aging_step: int = 1,
    ) -> None:
        self._store = JobStore(db_path)
        self._scheduler = Scheduler(
            self._store, handler, max_concurrency=max_concurrency,
            aging_interval=aging_interval, aging_step=aging_step,
        )
        self._default_max_retries = max_retries

    def submit(
        self,
        job_id: str,
        payload: str,
        max_retries: int | None = None,
        priority: str | int | None = None,
    ) -> tuple[Job, bool]:
        """提交作业；同 ID 重复提交返回已有作业（幂等去重）。

        priority 接受命名级别（high/normal/low）或任意整数，缺省为 normal。
        返回 (job, created)：created=False 表示命中幂等去重，不重复入队。
        """
        existing = self._store.get(job_id)
        if existing is not None:
            logger.info("job=%s duplicate submit ignored, status=%s",
                        job_id, existing.status.value)
            return existing, False
        now = time.time()
        job = Job(
            job_id=job_id,
            payload=payload,
            status=JobStatus.PENDING,
            max_retries=max_retries if max_retries is not None else self._default_max_retries,
            priority=normalize_priority(priority),
            created_at=now,
            updated_at=now,
            queued_at=now,
            history=["created"],
        )
        validate_transition(job_id, JobStatus.PENDING, JobStatus.QUEUED)
        job.status = JobStatus.QUEUED
        job.history.append("PENDING->QUEUED")
        self._store.save(job)
        logger.info("job=%s submitted, transition PENDING->QUEUED priority=%d",
                    job_id, job.priority)
        self._scheduler.notify()
        return job, True

    def get(self, job_id: str) -> Job | None:
        return self._store.get(job_id)

    def start(self) -> None:
        """启动服务：先恢复崩溃时中断的作业，再启动调度。"""
        recovered = self._store.recover_interrupted()
        for job_id in recovered:
            logger.info("job=%s recovered from interrupted RUNNING -> QUEUED", job_id)
        self._scheduler.start()

    def stop(self) -> None:
        self._scheduler.stop(drain=True)
        self._store.close()

    def wait_idle(self, timeout: float = 5.0) -> bool:
        return self._scheduler.wait_idle(timeout)

    def enter_maintenance(self) -> None:
        self._scheduler.enter_maintenance()

    def exit_maintenance(self) -> None:
        self._scheduler.exit_maintenance()

    @property
    def in_maintenance(self) -> bool:
        return self._scheduler.in_maintenance

    def running_count(self) -> int:
        return self._scheduler.running_count()
