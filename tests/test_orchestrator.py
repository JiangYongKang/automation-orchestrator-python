"""编排流程测试：重复提交、并发争用、失败分类与重试、维护窗口、崩溃恢复。"""
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


def make_service(handler, max_concurrency=2, max_retries=3, db_path=":memory:"):
    svc = OrchestratorService(
        handler, db_path=db_path, max_concurrency=max_concurrency, max_retries=max_retries
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
        assert len(retry_logs) == 2  # 前两次失败被分类为可重试
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
