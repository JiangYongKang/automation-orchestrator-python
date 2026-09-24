"""FastAPI 入口：在进程内托管本地任务编排器。

仅依赖系统能力与仓库已有依赖（fastapi + 标准库）。
状态模型与规则见 docs/orchestration.md。
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from orchestrator.errors import (
    InvalidStateTransitionError,
    JobNotFoundError,
    OrchestratorError,
)
from orchestrator.handlers import HandlerRegistry
from orchestrator.scheduler import Scheduler
from orchestrator.states import JobState
from orchestrator.store import JsonJobStore

STORE_PATH = Path(os.environ.get("ORCHESTRATOR_STORE", "./data/jobs.json"))
MAX_CONCURRENCY = int(os.environ.get("ORCHESTRATOR_MAX_CONCURRENCY", "2"))
MAX_ATTEMPTS = int(os.environ.get("ORCHESTRATOR_MAX_ATTEMPTS", "3"))


def _demo_healthcheck(payload: dict[str, Any]) -> dict[str, Any]:
    """示例作业：healthcheck，用 mode 演示失败分类。

    - ok    ：直接成功
    - fatal ：抛 FatalJobError（不可重试，立即失败）
    - flaky ：按 token 计数，前 fail_times 次抛 RetryableJobError，之后成功
    """
    mode = payload.get("mode", "ok")
    if mode == "fatal":
        from orchestrator.errors import FatalJobError
        raise FatalJobError("healthcheck permanently misconfigured",
                            code="fatal_error")
    if mode == "flaky":
        from orchestrator.errors import RetryableJobError

        fail_times = int(payload.get("fail_times", 2))
        token = payload.get("token", "default")
        if not hasattr(_demo_healthcheck, "_counters"):
            _demo_healthcheck._counters = {}
        counters = _demo_healthcheck._counters
        seen = counters.get(token, 0)
        counters[token] = seen + 1
        if seen < fail_times:
            raise RetryableJobError(f"transient failure {seen + 1}/{fail_times}")
    return {"checked": True, "mode": mode}


registry = HandlerRegistry()
registry.register("healthcheck", _demo_healthcheck)

scheduler = Scheduler(
    JsonJobStore(STORE_PATH),
    registry,
    max_concurrency=MAX_CONCURRENCY,
    default_max_attempts=MAX_ATTEMPTS,
)
# 进程启动恢复：中断在 RUNNING 的作业回到 PENDING
scheduler.recover()
scheduler.start()

app = FastAPI(title="local-automation-orchestrator", version="0.1.0")


class SubmitBody(BaseModel):
    job_id: str = Field(..., min_length=1)
    job_type: str = Field(..., min_length=1)
    payload: dict[str, Any] = Field(default_factory=dict)
    max_attempts: int | None = Field(default=None, ge=1)


class TransitionBody(BaseModel):
    to_state: JobState
    reason: str | None = None


def _job_dict(job) -> dict[str, Any]:
    d = job.to_dict()
    d["state"] = job.state.value
    return d


@app.get("/")
def read_root() -> dict[str, Any]:
    return {
        "service": "local-automation-orchestrator",
        "max_concurrency": MAX_CONCURRENCY,
        "active": scheduler.active_count,
        "maintenance": scheduler.in_maintenance,
        "store": str(STORE_PATH),
    }


@app.post("/jobs")
def submit_job(body: SubmitBody) -> dict[str, Any]:
    try:
        result = scheduler.submit(
            body.job_id, body.job_type, body.payload,
            max_attempts=body.max_attempts,
        )
    except OrchestratorError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"created": result.created, "job": _job_dict(result.job)}


@app.get("/jobs/{job_id}")
def get_job(job_id: str) -> dict[str, Any]:
    try:
        return _job_dict(scheduler.get(job_id))
    except JobNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get("/jobs")
def list_jobs() -> dict[str, Any]:
    return {"jobs": [_job_dict(j) for j in scheduler.store.all()]}


@app.post("/jobs/{job_id}/transition")
def force_transition(job_id: str, body: TransitionBody) -> dict[str, Any]:
    """显式驱动状态流转；非法流转返回 409 与可区分原因码。"""
    try:
        job = scheduler.get(job_id)
        scheduler.transition(job, body.to_state, reason=body.reason)
    except JobNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except InvalidStateTransitionError as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "error": "invalid_state_transition",
                "job_id": exc.job_id,
                "from_state": exc.from_state.value,
                "to_state": exc.to_state.value,
                "reason": exc.reason,
            },
        ) from exc
    return _job_dict(job)


@app.post("/maintenance/begin")
def begin_maintenance() -> dict[str, str]:
    scheduler.begin_maintenance()
    return {"maintenance": "begun", "policy": "drain"}


@app.post("/maintenance/end")
def end_maintenance() -> dict[str, str]:
    scheduler.end_maintenance()
    return {"maintenance": "ended"}
