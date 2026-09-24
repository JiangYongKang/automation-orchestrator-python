"""失败分类与重试退避策略。"""

from __future__ import annotations

from orchestrator.errors import FatalJobError, RetryableJobError

RETRYABLE = "retryable"
FATAL = "fatal"


def classify_error(exc: BaseException) -> str:
    """将执行异常归类为 ``retryable`` 或 ``fatal``。

    判定依据：
    - RetryableJobError -> retryable：调用方显式声明的临时性故障；
    - FatalJobError     -> fatal：调用方显式声明的永久性故障，立即失败；
    - 其它未知异常       -> retryable（保守策略：未知故障先重试，
      达到重试上限后再给出 FAILED 结论，避免一次偶发异常直接终结作业）。
    """
    if isinstance(exc, FatalJobError):
        return FATAL
    if isinstance(exc, RetryableJobError):
        return RETRYABLE
    return RETRYABLE


def backoff_delay(attempt: int, base: float = 0.1, factor: float = 2.0,
                  cap: float = 30.0) -> float:
    """指数退避：``min(base * factor**(attempt-1), cap)``，attempt 从 1 开始。

    例如 base=0.1, factor=2：0.1, 0.2, 0.4, 0.8, ... 直到 cap。
    """
    if attempt < 1:
        raise ValueError("attempt must be >= 1")
    delay = base * (factor ** (attempt - 1))
    return min(delay, cap)
