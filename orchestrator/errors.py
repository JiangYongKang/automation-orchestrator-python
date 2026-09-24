"""错误类型：非法流转、失败分类（可重试 / 不可重试）、作业/处理器不存在。"""

from __future__ import annotations

from typing import Any

from orchestrator.states import JobState


class OrchestratorError(Exception):
    """所有编排器错误的基类。"""


class InvalidStateTransitionError(OrchestratorError):
    """非法状态流转，携带作业标识、来源/目标状态与可区分原因码。

    reason 取值见 states.reject_reason：
    - terminal_state      终态不可逆（例如已完成作业被重新置为运行中）
    - illegal_transition  不存在对应合法路径
    - already_in_state    源状态与目标状态相同
    """

    def __init__(self, job_id: str, from_state: JobState, to_state: JobState,
                 reason: str):
        self.job_id = job_id
        self.from_state = from_state
        self.to_state = to_state
        self.reason = reason
        super().__init__(
            f"illegal transition for job {job_id!r}: "
            f"{from_state.value} -> {to_state.value} ({reason})"
        )


class RetryableJobError(OrchestratorError):
    """可重试错误（如临时网络抖动、资源暂时不可用）。"""

    error_class = "retryable"

    def __init__(self, message: str, *, details: dict[str, Any] | None = None):
        self.details = details or {}
        super().__init__(message)


class FatalJobError(OrchestratorError):
    """不可重试错误（如参数非法、前置条件永久不满足）。"""

    error_class = "fatal"

    def __init__(self, message: str, *, code: str | None = None,
                 details: dict[str, Any] | None = None):
        self.code = code
        self.details = details or {}
        super().__init__(message)


class JobNotFoundError(OrchestratorError):
    """作业不存在。"""

    def __init__(self, job_id: str):
        self.job_id = job_id
        super().__init__(f"job not found: {job_id!r}")


class HandlerNotFoundError(OrchestratorError):
    """作业类型未注册处理器。"""

    def __init__(self, job_type: str):
        self.job_type = job_type
        super().__init__(f"no handler registered for job type: {job_type!r}")
