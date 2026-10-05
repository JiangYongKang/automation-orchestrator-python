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


# 命名优先级档位：数值越小越优先
PRIORITY_HIGH = 0
PRIORITY_NORMAL = 5
PRIORITY_LOW = 10

# 等待超时后被提升到的“插队档”：高于一切普通/自定义档位
PRIORITY_BOOSTED = -1

_PRIORITY_NAMES: dict[str, int] = {
    "high": PRIORITY_HIGH,
    "normal": PRIORITY_NORMAL,
    "low": PRIORITY_LOW,
}

DEFAULT_PRIORITY = PRIORITY_NORMAL


def normalize_priority(priority: str | int | None) -> int:
    """把用户输入的优先级归一化为非负整数（越小越优先）。

    - None -> 默认 NORMAL
    - 命名档位（大小写不敏感）：high / normal / low
    - 非负整数：自定义档位；负值按 0（最高档）处理
    - 未知名称抛出 ValueError，其它类型抛出 TypeError
    """
    if priority is None:
        return DEFAULT_PRIORITY
    if isinstance(priority, bool):
        raise TypeError(f"invalid priority type: bool ({priority!r})")
    if isinstance(priority, int):
        return max(0, priority)
    if isinstance(priority, str):
        key = priority.strip().lower()
        if key in _PRIORITY_NAMES:
            return _PRIORITY_NAMES[key]
        raise ValueError(
            f"invalid priority {priority!r}: expected one of "
            f"{sorted(_PRIORITY_NAMES)} or a non-negative integer"
        )
    raise TypeError(f"invalid priority type: {type(priority).__name__}")


def effective_priority(job: Job, now: float, aging_threshold: float) -> int:
    """计算作业的有效优先级：等待超过 aging_threshold 秒即提升到插队档。

    防饿死（aging）规则：作业自最近一次入队（queued_at）起等待时间单调增长，
    一旦超过阈值即被提升为 PRIORITY_BOOSTED；提升档内部仍按 queued_at 先来先服务，
    因此晚入队的作业永远无法越过已提升的作业，任何作业最终都能被派发。
    """
    if aging_threshold >= 0 and now - job.queued_at >= aging_threshold:
        return PRIORITY_BOOSTED
    return job.priority


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
    priority: int = DEFAULT_PRIORITY  # 归一化后的非负整数，越小越优先
    failure_class: str | None = None
    conclusion: str | None = None
    created_at: float = 0.0
    updated_at: float = 0.0
    queued_at: float = 0.0  # 最近一次进入 QUEUED 的时刻，用于同级 FIFO 与防饿死计时
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
