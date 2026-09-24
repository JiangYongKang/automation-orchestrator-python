"""作业状态模型与失败分类的类型定义（骨架：仅类型与常量）。"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field


class JobStatus(str, enum.Enum):
    """作业生命周期状态。"""

    PENDING = "PENDING"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


TERMINAL_STATES: frozenset[JobStatus] = frozenset({JobStatus.SUCCEEDED, JobStatus.FAILED})

# 合法状态迁移表：key -> 允许的下一状态集合
ALLOWED_TRANSITIONS: dict[JobStatus, frozenset[JobStatus]] = {
    JobStatus.PENDING: frozenset({JobStatus.QUEUED}),
    JobStatus.QUEUED: frozenset({JobStatus.RUNNING}),
    JobStatus.RUNNING: frozenset({JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.QUEUED}),
    JobStatus.SUCCEEDED: frozenset(),
    JobStatus.FAILED: frozenset(),
}


class FailureClass(str, enum.Enum):
    """失败分类。"""

    RETRYABLE = "RETRYABLE"
    FATAL = "FATAL"


class InvalidTransitionError(Exception):
    """非法状态迁移。reason 字段给出可区分的原因。"""

    def __init__(self, job_id: str, current: JobStatus, target: JobStatus, reason: str) -> None:
        self.job_id = job_id
        self.current = current
        self.target = target
        self.reason = reason
        super().__init__(f"job={job_id} {current.value}->{target.value} rejected: {reason}")


class RetryableError(Exception):
    """可重试的执行错误。"""


class FatalError(Exception):
    """不可重试的执行错误。"""


@dataclass
class Job:
    """作业记录。"""

    job_id: str
    payload: str
    status: JobStatus = JobStatus.PENDING
    attempts: int = 0
    max_retries: int = 3
    failure_class: str | None = None
    conclusion: str | None = None
    created_at: float = 0.0
    updated_at: float = 0.0
    not_before: float = 0.0  # 退避后的最早可调度时间
    history: list[str] = field(default_factory=list)


def validate_transition(job_id: str, current: JobStatus, target: JobStatus) -> None:
    """校验状态迁移合法性，非法时抛出带可区分原因的 InvalidTransitionError。"""
    if current in TERMINAL_STATES:
        raise InvalidTransitionError(
            job_id, current, target,
            reason=f"job already in terminal state {current.value}",
        )
    if target not in ALLOWED_TRANSITIONS[current]:
        raise InvalidTransitionError(
            job_id, current, target,
            reason=f"transition {current.value}->{target.value} is not allowed",
        )
