"""数据模型：作业、事件、提交结果。"""

from __future__ import annotations

import dataclasses
from typing import Any

from orchestrator.states import JobState


@dataclasses.dataclass
class JobEvent:
    """作业事件流中的一条记录（提交、状态迁移、失败分类、重试决策等）。"""

    ts: float                       # 事件时间（epoch 秒）
    kind: str                       # submitted / transition / retry / fatal / ...
    from_state: str | None = None
    to_state: str | None = None
    attempt: int = 0
    error_class: str | None = None  # retryable / fatal
    reason: str | None = None
    detail: dict[str, Any] = dataclasses.field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ts": self.ts,
            "kind": self.kind,
            "from_state": self.from_state,
            "to_state": self.to_state,
            "attempt": self.attempt,
            "error_class": self.error_class,
            "reason": self.reason,
            "detail": self.detail,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> JobEvent:
        return cls(
            ts=float(data["ts"]),
            kind=data["kind"],
            from_state=data.get("from_state"),
            to_state=data.get("to_state"),
            attempt=int(data.get("attempt", 0)),
            error_class=data.get("error_class"),
            reason=data.get("reason"),
            detail=dict(data.get("detail") or {}),
        )


@dataclasses.dataclass
class Job:
    """一个运维作业的完整持久化状态。"""

    job_id: str
    job_type: str
    payload: dict[str, Any]
    state: JobState = JobState.PENDING
    submit_seq: int = 0                 # 提交序号，决定 FIFO 排队顺序
    attempts: int = 0                   # 已执行（含正在执行）次数
    max_attempts: int = 3
    run_after: float = 0.0              # 退避结束时间（epoch 秒）
    created_at: float = 0.0
    updated_at: float = 0.0
    started_at: float | None = None
    finished_at: float | None = None
    result: dict[str, Any] | None = None
    failure_reason: str | None = None   # 可追踪的失败结论码
    failure_message: str | None = None
    events: list[JobEvent] = dataclasses.field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "job_type": self.job_type,
            "payload": self.payload,
            "state": self.state.value,
            "submit_seq": self.submit_seq,
            "attempts": self.attempts,
            "max_attempts": self.max_attempts,
            "run_after": self.run_after,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "result": self.result,
            "failure_reason": self.failure_reason,
            "failure_message": self.failure_message,
            "events": [e.to_dict() for e in self.events],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Job:
        return cls(
            job_id=data["job_id"],
            job_type=data["job_type"],
            payload=dict(data.get("payload") or {}),
            state=JobState(data["state"]),
            submit_seq=int(data.get("submit_seq", 0)),
            attempts=int(data.get("attempts", 0)),
            max_attempts=int(data.get("max_attempts", 3)),
            run_after=float(data.get("run_after", 0.0)),
            created_at=float(data.get("created_at", 0.0)),
            updated_at=float(data.get("updated_at", 0.0)),
            started_at=data.get("started_at"),
            finished_at=data.get("finished_at"),
            result=data.get("result"),
            failure_reason=data.get("failure_reason"),
            failure_message=data.get("failure_message"),
            events=[JobEvent.from_dict(e) for e in data.get("events", [])],
        )


@dataclasses.dataclass
class SubmitResult:
    """提交结果：幂等提交可区分「新建」与「已存在」。"""

    job: Job
    created: bool
