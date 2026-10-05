"""编排流程测试：重复提交、并发争用、失败分类与重试、维护窗口、崩溃恢复、优先级调度与防饿死。"""
import logging
import threading
import time

import pytest

from orchestrator.models import (
    FatalError,
    InvalidTransitionError,
    JobStatus,
    RetryableError,
    validate_transition,
)
from orchestrator.service import OrchestratorService
from orchestrator.store import JobStore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(message)s",
)
logger = logging.getLogger("test")


def make_service(handler, max_concurrency=2, max_retries=3, db_path=":memory:",
                 aging_threshold=30.0):
    svc = OrchestratorService(
        handler, db_path=db_path, max_concurrency=max_concurrency,
        max_retries=max_retries, aging_threshold=aging_threshold,
    )
    svc.start()
    return svc


def wait_status(svc, job_id, status, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        job = svc.get(job_id)
        if job and job.status == status:
            return job
        time.sleep(0.01)
    raise AssertionError(f"job {job_id} did not reach {status}, got {svc.get(job_id).status}")


def test_duplicate_submit_is_idempotent():
    svc = make_service(lambda p: None)
    try:
        job1, created1 = svc.submit("job-dup", "payload")
        job2, created2 = svc.submit("job-dup", "payload")
        assert created1 is True
        assert created2 is False
        assert job1.job_id == job2.job_id
        final = wait_status(svc, "job-dup", JobStatus.SUCCEEDED)
        logger.info("job=%s final=%s attempts=%d", final.job_id, final.status, final.attempts)
        assert final.attempts == 1  # 只执行了一次
    finally:
        svc.stop()


def test_concurrency_limit_and_fifo_queue():
    started, finished = [], []
    lock = threading.Lock()
    release = threading.Event()

    def handler(payload):
        with lock:
            started.append(payload)
        release.wait(timeout=5)
        with lock:
            finished.append(payload)

    svc = make_service(handler, max_concurrency=2)
    try:
        for i in range(4):
            svc.submit(f"job-{i}", f"job-{i}")
        # 等前两个进入 RUNNING
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and len(started) < 2:
            time.sleep(0.01)
        assert sorted(started) == ["job-0", "job-1"]
        assert svc.running_count() == 2
        # 超额的作业在队列中而非被拒绝
        assert svc.get("job-2").status == JobStatus.QUEUED
        assert svc.get("job-3").status == JobStatus.QUEUED
        release.set()
        assert svc.wait_idle()
        assert sorted(finished) == [f"job-{i}" for i in range(4)]
        # FIFO：派发（启动）顺序按提交顺序，完成顺序对并发作业不作假设
        assert started == ["job-0", "job-1", "job-2", "job-3"]
        logger.info("fifo dispatch order: %s, finished: %s", started, finished)
    finally:
        release.set()
        svc.stop()


def test_retryable_failure_retries_then_succeeds(caplog):
    attempts = {"n": 0}

    def handler(payload):
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise RetryableError("transient backend error")

    svc = make_service(handler, max_retries=3)
    try:
        with caplog.at_level(logging.INFO, logger="orchestrator.scheduler"):
            svc.submit("job-retry", "x")
            job = wait_status(svc, "job-retry", JobStatus.SUCCEEDED)
        assert attempts["n"] == 3
        assert job.attempts == 3
        logger.info("job=%s succeeded after %d attempts", job.job_id, job.attempts)
        retry_logs = [
            r for r in caplog.records
            if r.name == "orchestrator.scheduler" and "class=RETRYABLE" in r.getMessage()
        ]
        assert len(retry_logs) == 2  # 前两次失败被分类为可重试（仅统计调度器的重试日志）
    finally:
        svc.stop()


def test_retry_limit_gives_traceable_conclusion():
    def handler(payload):
        raise RetryableError("always failing")

    svc = make_service(handler, max_retries=2)
    try:
        svc.submit("job-exhaust", "x")
        job = wait_status(svc, "job-exhaust", JobStatus.FAILED)
        assert job.attempts == 3  # 首次 + 2 次重试
        assert job.failure_class == "RETRYABLE"
        assert "retry limit reached (2)" in job.conclusion
        assert "always failing" in job.conclusion
        logger.info("job=%s conclusion=%s", job.job_id, job.conclusion)
    finally:
        svc.stop()


def test_fatal_failure_not_retried():
    calls = {"n": 0}

    def handler(payload):
        calls["n"] += 1
        raise FatalError("bad config")

    svc = make_service(handler)
    try:
        svc.submit("job-fatal", "x")
        job = wait_status(svc, "job-fatal", JobStatus.FAILED)
        assert calls["n"] == 1  # 不可重试，只执行一次
        assert job.failure_class == "FATAL"
        assert "non-retryable" in job.conclusion
        logger.info("job=%s class=FATAL conclusion=%s", job.job_id, job.conclusion)
    finally:
        svc.stop()


def test_one_failure_does_not_affect_others():
    def handler(payload):
        if payload == "bad":
            raise FatalError("broken")
        time.sleep(0.05)

    svc = make_service(handler)
    try:
        svc.submit("job-bad", "bad")
        svc.submit("job-good", "good")
        assert wait_status(svc, "job-bad", JobStatus.FAILED)
        good = wait_status(svc, "job-good", JobStatus.SUCCEEDED)
        assert good.conclusion == "ok"
        # 调度器仍然可用
        svc.submit("job-after", "ok")
        assert wait_status(svc, "job-after", JobStatus.SUCCEEDED)
    finally:
        svc.stop()


def test_illegal_transition_rejected_with_reason():
    with pytest.raises(InvalidTransitionError) as exc:
        validate_transition("job-x", JobStatus.SUCCEEDED, JobStatus.RUNNING)
    assert "terminal state" in exc.value.reason
    logger.info("job=job-x rejected: %s", exc.value.reason)
    with pytest.raises(InvalidTransitionError) as exc2:
        validate_transition("job-y", JobStatus.PENDING, JobStatus.RUNNING)
    assert "not allowed" in exc2.value.reason


def test_maintenance_window_blocks_new_starts_and_drains():
    started = []
    release = threading.Event()

    def handler(payload):
        started.append(payload)
        release.wait(timeout=5)

    svc = make_service(handler, max_concurrency=1)
    try:
        svc.submit("job-inflight", "inflight")
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and not started:
            time.sleep(0.01)
        assert started == ["inflight"]
        svc.enter_maintenance()
        svc.submit("job-queued", "queued")
        time.sleep(0.2)
        # 窗口内不启动新作业
        assert svc.get("job-queued").status == JobStatus.QUEUED
        # 在途作业可跑完（drain 语义）
        release.set()
        wait_status(svc, "job-inflight", JobStatus.SUCCEEDED)
        time.sleep(0.2)
        assert svc.get("job-queued").status == JobStatus.QUEUED
        logger.info("maintenance: inflight drained, job-queued held")
        svc.exit_maintenance()
        wait_status(svc, "job-queued", JobStatus.SUCCEEDED)
    finally:
        release.set()
        svc.stop()


def test_crash_recovery_requeues_interrupted(tmp_path):
    db = str(tmp_path / "jobs.db")
    store = JobStore(db)
    from orchestrator.models import Job
    now = time.time()
    store.save(Job(job_id="job-stuck", payload="p", status=JobStatus.RUNNING,
                   attempts=1, created_at=now, updated_at=now))
    store.close()
    # 模拟重启：RUNNING 中的作业被恢复为 QUEUED 并重新执行
    done = []
    svc = make_service(lambda p: done.append(p), db_path=db)
    try:
        job = wait_status(svc, "job-stuck", JobStatus.SUCCEEDED)
        assert done == ["p"]
        logger.info("job=%s recovered and completed, attempts=%d", job.job_id, job.attempts)
    finally:
        svc.stop()


# ---- 优先级调度 ----


def test_priority_dispatch_order():
    """不同优先级：高的先派发；并发上限不被优先级突破。"""
    started = []
    release = threading.Event()

    def handler(payload):
        started.append(payload)
        release.wait(timeout=5)

    svc = make_service(handler, max_concurrency=1)
    try:
        svc.submit("blocker", "blocker")  # 占住唯一执行槽
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and not started:
            time.sleep(0.01)
        assert started == ["blocker"]
        # 按 低->普通->高 的相反顺序提交，验证不是按提交顺序派发
        svc.submit("job-low", "low", priority="low")
        svc.submit("job-normal", "normal", priority="normal")
        svc.submit("job-high", "high", priority="high")
        time.sleep(0.1)
        # 优先级不能突破并发上限：仍然只有 1 个在跑
        assert svc.running_count() == 1
        assert svc.get("job-high").status == JobStatus.QUEUED
        release.set()
        assert svc.wait_idle()
        assert started == ["blocker", "high", "normal", "low"]
        logger.info("priority dispatch order: %s", started)
    finally:
        release.set()
        svc.stop()


def test_same_priority_is_fifo():
    """同优先级内部先来先服务，不被优先级机制打乱。"""
    started = []
    release = threading.Event()

    def handler(payload):
        started.append(payload)
        release.wait(timeout=5)

    svc = make_service(handler, max_concurrency=1)
    try:
        svc.submit("blocker", "blocker")
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and not started:
            time.sleep(0.01)
        for i in range(4):
            svc.submit(f"job-{i}", f"p{i}", priority="low")
        release.set()
        assert svc.wait_idle()
        assert started == ["blocker", "p0", "p1", "p2", "p3"]
        logger.info("same-priority fifo order: %s", started)
    finally:
        release.set()
        svc.stop()


def test_low_priority_eventually_dispatched_under_high_pressure():
    """防饿死：高优先级持续涌入时，低优先级等待超过 aging 阈值后被提升并派发。"""
    started = []
    lock = threading.Lock()
    release = threading.Event()

    def handler(payload):
        if payload == "blocker":
            release.wait(timeout=5)
            return
        time.sleep(0.05)  # 每个高优先级作业占用执行槽 50ms
        with lock:
            started.append(payload)

    # aging 阈值 0.3s：20 个高优先级作业共需 ~1s，无 aging 时低优先级会排到最后
    svc = make_service(handler, max_concurrency=1, aging_threshold=0.3)
    try:
        svc.submit("blocker", "blocker")
        svc.submit("job-low", "low", priority="low")
        n_high = 20
        for i in range(n_high):
            svc.submit(f"job-high-{i}", f"high-{i}", priority="high")
        release.set()
        assert svc.wait_idle(timeout=15)
        low_pos = started.index("low")
        last_high_pos = started.index(f"high-{n_high - 1}")
        # 低优先级被提升后插到了队首高优先级之后、队尾高优先级之前
        assert low_pos < last_high_pos, (
            f"low starved: low at {low_pos}, last high at {last_high_pos}"
        )
        logger.info("anti-starvation: low dispatched at position %d/%d (last high at %d)",
                    low_pos, len(started), last_high_pos)
    finally:
        release.set()
        svc.stop()


def test_maintenance_window_blocks_high_priority_too():
    """维护窗口期间高优先级也不能插队启动。"""
    svc = make_service(lambda p: None, max_concurrency=1)
    try:
        svc.enter_maintenance()
        svc.submit("job-urgent", "x", priority="high")
        time.sleep(0.2)
        assert svc.get("job-urgent").status == JobStatus.QUEUED
        svc.exit_maintenance()
        wait_status(svc, "job-urgent", JobStatus.SUCCEEDED)
    finally:
        svc.exit_maintenance()
        svc.stop()


def test_retried_job_keeps_priority_and_requeues_fresh():
    """重试重新入队：保留原优先级，queued_at 重置，退避后仍能被调度到。"""
    attempts = {"n": 0}

    def handler(payload):
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise RetryableError("transient")

    svc = make_service(handler, max_retries=2)
    try:
        svc.submit("job-retry-prio", "x", priority="high")
        job = wait_status(svc, "job-retry-prio", JobStatus.SUCCEEDED)
        assert job.attempts == 2
        assert job.priority == 0  # 重试不丢优先级
        assert job.queued_at > job.created_at  # 重新入队时 queued_at 已重置
        logger.info("job=%s retried with priority=%d, queued_at reset",
                    job.job_id, job.priority)
    finally:
        svc.stop()


def test_invalid_priority_rejected():
    """非法优先级取值被拒绝，且不会创建作业。"""
    svc = make_service(lambda p: None)
    try:
        with pytest.raises(ValueError, match="invalid priority"):
            svc.submit("job-bad-prio", "x", priority="urgent")
        assert svc.get("job-bad-prio") is None
        with pytest.raises(TypeError, match="invalid priority type"):
            svc.submit("job-bad-prio2", "x", priority=True)
        assert svc.get("job-bad-prio2") is None
        # 数值优先级：越小越优先，负值按 0 处理
        job, _ = svc.submit("job-num", "x", priority=3)
        assert job.priority == 3
        job2, _ = svc.submit("job-neg", "x", priority=-5)
        assert job2.priority == 0
    finally:
        svc.stop()


def test_duplicate_submit_keeps_original_priority():
    """幂等去重不受优先级影响：重复提交保留首次提交的优先级。"""
    svc = make_service(lambda p: None)
    try:
        job1, created1 = svc.submit("job-dup-prio", "x", priority="low")
        job2, created2 = svc.submit("job-dup-prio", "x", priority="high")
        assert created1 is True and created2 is False
        assert job2.priority == job1.priority == 10  # 仍是首次提交的 low
        wait_status(svc, "job-dup-prio", JobStatus.SUCCEEDED)
    finally:
        svc.stop()
