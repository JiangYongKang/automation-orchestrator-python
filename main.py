"""HTTP 入口：把编排服务暴露为 REST API。"""
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from orchestrator.service import OrchestratorService

logging.basicConfig(level=logging.INFO)

# 演示用处理器：payload 为 "retryable"/"fatal" 时模拟对应失败，否则视为成功
from orchestrator.models import FatalError, RetryableError


def demo_handler(payload: str) -> None:
    if payload == "retryable":
        raise RetryableError("simulated transient failure")
    if payload == "fatal":
        raise FatalError("simulated permanent failure")


service = OrchestratorService(
    demo_handler,
    db_path=os.environ.get("ORCH_DB", ":memory:"),
    max_concurrency=int(os.environ.get("ORCH_MAX_CONCURRENCY", "2")),
    aging_interval=float(os.environ.get("ORCH_AGING_INTERVAL", "1.0")),
    aging_step=int(os.environ.get("ORCH_AGING_STEP", "1")),
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    service.start()
    yield
    service.stop()


app = FastAPI(lifespan=lifespan)


class SubmitRequest(BaseModel):
    job_id: str
    payload: str
    max_retries: int | None = None
    priority: str | int | None = None  # high/normal/low 或任意整数，缺省 normal


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.post("/jobs", status_code=201)
def submit_job(req: SubmitRequest):
    try:
        job, created = service.submit(req.job_id, req.payload, req.max_retries, req.priority)
    except (ValueError, TypeError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {
        "job_id": job.job_id,
        "status": job.status.value,
        "priority": job.priority,
        "created": created,  # False 表示幂等去重命中
    }


@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    job = service.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return {
        "job_id": job.job_id,
        "status": job.status.value,
        "attempts": job.attempts,
        "priority": job.priority,
        "failure_class": job.failure_class,
        "conclusion": job.conclusion,
        "history": job.history,
    }


@app.post("/maintenance/enter")
def enter_maintenance():
    service.enter_maintenance()
    return {"maintenance": True}


@app.post("/maintenance/exit")
def exit_maintenance():
    service.exit_maintenance()
    return {"maintenance": False}
