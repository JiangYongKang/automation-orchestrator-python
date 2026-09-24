"""执行器：运行作业处理器并按异常类型分类失败。"""
from __future__ import annotations

import logging
from typing import Callable

from .models import FatalError, FailureClass, RetryableError

logger = logging.getLogger("orchestrator.executor")

# 作业处理器签名：接收 payload，返回 None 或抛出 RetryableError/FatalError
JobHandler = Callable[[str], None]


class ExecutionResult:
    """单次执行的结论。"""

    def __init__(self, succeeded: bool, failure_class: FailureClass | None, detail: str) -> None:
        self.succeeded = succeeded
        self.failure_class = failure_class
        self.detail = detail

    def __repr__(self) -> str:  # pragma: no cover
        return f"ExecutionResult(ok={self.succeeded}, class={self.failure_class}, {self.detail!r})"


def classify_exception(exc: BaseException) -> FailureClass:
    """把异常分类为可重试 / 不可重试。

    RetryableError 及超时/连接类系统错误视为可重试；FatalError 及其余视为不可重试。
    """
    if isinstance(exc, FatalError):
        return FailureClass.FATAL
    if isinstance(exc, (RetryableError, TimeoutError, ConnectionError, OSError)):
        return FailureClass.RETRYABLE
    return FailureClass.FATAL


def execute(handler: JobHandler, payload: str) -> ExecutionResult:
    """执行一次作业，捕获异常并分类，绝不向上抛出。"""
    try:
        handler(payload)
        return ExecutionResult(True, None, "ok")
    except Exception as exc:  # noqa: BLE001 - 需要兜底分类
        failure_class = classify_exception(exc)
        logger.info("execution failed: class=%s detail=%s", failure_class.value, exc)
        return ExecutionResult(False, failure_class, f"{type(exc).__name__}: {exc}")
