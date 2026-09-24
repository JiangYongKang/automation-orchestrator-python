"""作业处理器注册表：按 job_type 解析执行函数。"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from orchestrator.errors import HandlerNotFoundError

# 处理函数：接收 payload，返回结果 dict；抛 RetryableJobError / FatalJobError
# 表达失败分类，抛其它异常按可重试保守处理。
JobHandler = Callable[[dict[str, Any]], dict[str, Any]]


class HandlerRegistry:
    """作业类型 -> 处理函数 的注册表。"""

    def __init__(self) -> None:
        self._handlers: dict[str, JobHandler] = {}

    def register(self, job_type: str, handler: JobHandler) -> None:
        if not isinstance(job_type, str) or not job_type:
            raise ValueError("job_type must be a non-empty string")
        if not callable(handler):
            raise TypeError("handler must be callable")
        self._handlers[job_type] = handler

    def resolve(self, job_type: str) -> JobHandler:
        try:
            return self._handlers[job_type]
        except KeyError:
            raise HandlerNotFoundError(job_type) from None
