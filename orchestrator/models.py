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


# 命名优先级 -> 数值（数值越大越优先；也允许直接提交任意整数）
PRIORITY_LEVELS: dict[str, int] = {
    "low": -10,
    "normal": 0,
    "high": 10,
}

DEFAULT_PRIORITY: int = PRIORITY_LEVELS["normal"]


def normalize_priority(priority: str | int | None) -> int:
    """把提交时的优先级规范化为整数。

    接受命名级别（high/normal/low，大小写不敏感）或任意整数；
    None 表示未指定，取默认 normal。类型不合法抛 TypeError，
    字符串无法解析抛 ValueError。
    """
    if priority is None:
        return DEFAULT_PRIORITY
    if isinstance(priority, bool):  # bool 是 int 子类，单独拒绝避免歧义
        raise TypeError(f"invalid priority type: {priority!r}")
    if isinstance(priority, int):
        return priority
    if isinstance(priority, str):
        key = priority.strip().lower()
        if key in PRIORITY_LEVELS:
            return PRIORITY_LEVELS[key]
        try:
            return int(key)
        except ValueError:
            pass
        raise ValueError(
            f"invalid priority: {priority!r}; "
            f"expected one of {sorted(PRIORITY_LEVELS)} or an integer"
        ) from None
    raise TypeError(f"invalid priority type: {type(priority).__name__}")


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
    priority: int = DEFAULT_PRIORITY  # 数值越大越优先
    failure_class: str | None = None
    conclusion: str | None = None
    created_at: float = 0.0
    updated_at: float = 0.0
    queued_at: float = 0.0  # 最近一次进入队列的时间（防饿死老化计时起点）
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
