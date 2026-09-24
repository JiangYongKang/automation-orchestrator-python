"""作业状态模型与合法状态流转表。

状态图：

    PENDING ──调度启动──▶ RUNNING ──成功──────────────▶ SUCCEEDED(终态)
       ▲                   │
       │                   ├──不可重试/重试耗尽──────▶ FAILED(终态)
       │                   │
       └──退避结束── WAITING_RETRY ◀──可重试失败──────┘

说明：
- RUNNING -> PENDING 不属于常规流转，仅在进程重启恢复（recover）时发生，
  由 Scheduler.recover() 强制重置并记录原因 ``interrupted_recovery``。
- 终态（SUCCEEDED / FAILED）不可逆，任何迁出都会被拒绝。
"""

from __future__ import annotations

import enum


class JobState(str, enum.Enum):
    """作业生命周期状态。"""

    PENDING = "pending"                  # 已提交，在队列中等待调度
    RUNNING = "running"                  # 已被调度执行
    WAITING_RETRY = "waiting_retry"      # 可重试失败，等待退避后重新入队
    SUCCEEDED = "succeeded"              # 终态：成功
    FAILED = "failed"                    # 终态：失败（不可重试或重试耗尽）


TERMINAL_STATES = frozenset({JobState.SUCCEEDED, JobState.FAILED})


# 常规合法流转（不含恢复特例）
_ALLOWED: dict[JobState, frozenset[JobState]] = {
    JobState.PENDING: frozenset({JobState.RUNNING}),
    JobState.RUNNING: frozenset({
        JobState.SUCCEEDED,
        JobState.FAILED,
        JobState.WAITING_RETRY,
    }),
    JobState.WAITING_RETRY: frozenset({JobState.PENDING}),
    JobState.SUCCEEDED: frozenset(),
    JobState.FAILED: frozenset(),
}

# 非法流转的可区分原因码
REASON_ALREADY_IN_STATE = "already_in_state"   # 源状态与目标状态相同
REASON_TERMINAL_STATE = "terminal_state"       # 从终态迁出（如已完成再置运行中）
REASON_ILLEGAL_TRANSITION = "illegal_transition"  # 非终态但无此合法路径


def reject_reason(src: JobState, dst: JobState) -> str | None:
    """返回流转非法的原因码；合法时返回 None。"""
    if src == dst:
        return REASON_ALREADY_IN_STATE
    if src in TERMINAL_STATES:
        return REASON_TERMINAL_STATE
    if dst not in _ALLOWED.get(src, frozenset()):
        return REASON_ILLEGAL_TRANSITION
    return None


def can_transition(src: JobState, dst: JobState) -> bool:
    """判断常规流转是否合法。"""
    return reject_reason(src, dst) is None
