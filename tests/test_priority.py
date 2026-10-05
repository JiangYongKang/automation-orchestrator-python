"""优先级调度测试：派发顺序、同级 FIFO、防饿死老化、与既有规则的兼容。"""
import logging
import threading
import time

import pytest

from orchestrator.models import Job, JobStatus, normalize_priority
from orchestrator.scheduler import effective_priority
from orchestrator.service import OrchestratorService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(message)s",
)
logger = logging.getLogger("test.priority")


def make_service(handler, max_concurrency=2, aging_interval=1.0, aging_step=1, **kwargs):
    svc = OrchestratorService(
        handler, max_concurrency=max_concurrency,
        aging_interval=aging_interval, aging_step=aging_step, **kwargs
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


def wait_started(started, n, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline and len(started) < n:
        time.sleep(0.01)
    if len(started) < n:
        raise AssertionError(f"only {len(started)} jobs started: {started}")


# ---- 纯函数 / 参数解析 ----

def test_normalize_priority_named_and_numeric():
    assert normalize_priority(None) == 0
    assert normalize_priority("high") == 10
    assert normalize_priority("NORMAL") == 0
    assert normalize_priority(" low ") == -10
    assert normalize_priority(7) == 7
    assert normalize_priority("-3") == -3
    with pytest.raises(ValueError):
        normalize_priority("urgent")
    with pytest.raises(TypeError):
        normalize_priority(True)
    logger.info("priority normalization: named levels, integers, invalid rejected")


def test_effective_priority_ages_with_wait():
    job = Job(job_id="j", payload="p", priority=-10, queued_at=100.0)
    # 未满一个老化周期不加分
    assert effective_priority(job, now=100.4, aging_interval=0.5, aging_step=1) == -10
    # 每等待满一个周期 +aging_step
    assert effective_priority(job, now=101.0, aging_interval=0.5, aging_step=1) == -8
    # 等待足够久后低优先级反超新来的高优先级（10）
    assert effective_priority(job, now=111.0, aging_interval=0.5, aging_step=1) == 12
    logger.info("aging: effective priority grows unboundedly with wait time")


# ---- 派发顺序 ----

def test_high_priority_dispatched_before_lower():
    """维护窗口内入队不同优先级作业，窗口退出后高优先级先派发。"""
    started = []
    lock = threading.Lock()

    def handler(payload):
        with lock:
            started.append(payload)

    svc = make_service(handler, max_concurrency=1)
    try:
        svc.enter_maintenance()
        svc.submit("job-low", "low", priority="low")
        svc.submit("job-normal", "normal")  # 缺省 normal
        svc.submit("job-high", "high", priority="high")
        svc.exit_maintenance()
        wait_started(started, 3)
        assert started == ["high", "normal", "low"]
        logger.info("priority dispatch order: %s", started)
    finally:
        svc.stop()


def test_same_priority_fifo_preserved():
    """同一优先级内部仍按提交顺序先来先服务。"""
    started = []
    lock = threading.Lock()

    def handler(payload):
        with lock:
            started.append(payload)

    svc = make_service(handler, max_concurrency=1)
    try:
        svc.enter_maintenance()
        for i in range(4):
            svc.submit(f"job-{i}", f"p{i}", priority="high")
        svc.exit_maintenance()
        wait_started(started, 4)
        assert started == ["p0", "p1", "p2", "p3"]
        logger.info("same-priority FIFO order: %s", started)
    finally:
        svc.stop()


def test_numeric_priority_ordering():
    """直接给数值优先级：数值大的先派发。"""
    started = []
    lock = threading.Lock()

    def handler(payload):
        with lock:
            started.append(payload)

    svc = make_service(handler, max_concurrency=1)
    try:
        svc.enter_maintenance()
        svc.submit("job-p1", "p1", priority=1)
        svc.submit("job-p9", "p9", priority=9)
        svc.submit("job-p5", "p5", priority=5)
        svc.exit_maintenance()
        wait_started(started, 3)
        assert started == ["p9", "p5", "p1"]
        logger.info("numeric priority order: %s", started)
    finally:
        svc.stop()


# ---- 防饿死 ----

def test_low_priority_not_starved_by_continuous_high_influx():
    """高优先级作业持续涌入时，低优先级作业靠老化最终仍被调度到。"""
    started = []
    lock = threading.Lock()
    blocker_release = threading.Event()

    def handler(payload):
        with lock:
            started.append(payload)
        if payload == "blocker":
            blocker_release.wait(timeout=5)
        else:
            # 执行耗时长于投喂间隔，保证高优先级队列始终饱和：
            # low 只有在老化到反超 high 时才可能被派发
            time.sleep(0.05)

    # 老化周期 50ms：low(-10) 等待满 1.0s 后有效优先级追平 high(10)，
    # 同级按入队时间 FIFO，low 入队更早而获胜
    svc = make_service(handler, max_concurrency=1, aging_interval=0.05, aging_step=1)
    try:
        svc.submit("job-blocker", "blocker", priority="high")
        wait_started(started, 1)
        svc.submit("job-low", "low", priority="low")
        t_low_submit = time.monotonic()
        # 先放两个高优先级进队列，保证 low 不会靠“抢跑”获胜
        svc.submit("job-high-0", "high-0", priority="high")
        svc.submit("job-high-1", "high-1", priority="high")
        blocker_release.set()
        # 持续涌入高优先级作业（投喂快于消耗，队列始终不空），直到 low 被派发
        n_high = 2
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            with lock:
                if "low" in started:
                    break
            svc.submit(f"job-high-{n_high}", f"high-{n_high}", priority="high")
            n_high += 1
            time.sleep(0.02)
        with lock:
            low_index = started.index("low")
            assert started.index("high-0") < low_index
            assert started.index("high-1") < low_index
        waited = time.monotonic() - t_low_submit
        # 防饿死：持续高压下 low 仍被派发到（未死等到 deadline）
        # 且它不是提前插队：必须等老化积累到约 1.0s 后才可能反超 high
        assert waited >= 0.9
        wait_status(svc, "job-low", JobStatus.SUCCEEDED)
        logger.info("low dispatched at position %d after %.2fs wait, %d high submissions",
                    low_index, waited, n_high)
    finally:
        blocker_release.set()
        svc.stop()


# ---- 与既有规则的兼容 ----

def test_priority_respects_concurrency_limit():
    """高优先级不能突破并发上限，只能在余量内先派发。"""
    started = []
    lock = threading.Lock()
    release = threading.Event()
    peak = {"n": 0}
    running = {"n": 0}

    def handler(payload):
        with lock:
            started.append(payload)
            running["n"] += 1
            peak["n"] = max(peak["n"], running["n"])
        release.wait(timeout=5)
        with lock:
            running["n"] -= 1

    svc = make_service(handler, max_concurrency=2)
    try:
        svc.enter_maintenance()
        for i in range(3):
            svc.submit(f"job-low-{i}", f"low-{i}", priority="low")
        for i in range(3):
            svc.submit(f"job-high-{i}", f"high-{i}", priority="high")
        svc.exit_maintenance()
        wait_started(started, 2)
        # 先占满两个并发槽的是高优先级，且并发数从未超限
        assert sorted(started) == ["high-0", "high-1", "high-2"][:2]
        assert svc.running_count() == 2
        release.set()
        assert svc.wait_idle()
        assert peak["n"] <= 2
        assert started[:2] == ["high-0", "high-1"] or set(started[:2]) == {"high-0", "high-1"}
        logger.info("concurrency respected: peak=%d order=%s", peak["n"], started)
    finally:
        release.set()
        svc.stop()


def test_maintenance_window_blocks_high_priority_too():
    """维护窗口内高优先级也不能插队启动。"""
    started = []

    def handler(payload):
        started.append(payload)

    svc = make_service(handler, max_concurrency=1)
    try:
        svc.enter_maintenance()
        svc.submit("job-high", "high", priority="high")
        time.sleep(0.2)
        assert svc.get("job-high").status == JobStatus.QUEUED
        assert started == []
        svc.exit_maintenance()
        wait_status(svc, "job-high", JobStatus.SUCCEEDED)
        assert started == ["high"]
        logger.info("maintenance: high priority held until window exited")
    finally:
        svc.stop()


def test_retry_keeps_priority_and_requeues_ahead_of_low():
    """重试重新入队：保留原优先级，退避到期后按原优先级参与调度。"""
    from orchestrator.models import RetryableError

    started = []
    lock = threading.Lock()
    blocker_release = threading.Event()
    low_release = threading.Event()
    flaky_calls = {"n": 0}

    def handler(payload):
        with lock:
            started.append(payload)
        if payload == "blocker":
            blocker_release.wait(timeout=5)
        elif payload == "low":
            low_release.wait(timeout=5)
        elif payload == "flaky":
            flaky_calls["n"] += 1
            if flaky_calls["n"] == 1:
                raise RetryableError("transient")

    svc = make_service(handler, max_concurrency=1)
    try:
        svc.submit("job-blocker", "blocker", priority="high")
        wait_started(started, 1)
        # 低优先级先入队，高优先级但会失败重试的作业后入队
        svc.submit("job-low", "low", priority="low")
        svc.submit("job-flaky", "flaky", priority="high")
        blocker_release.set()
        # flaky 先派发（高优先级），失败后退避重新入队；
        # 退避期间 low 作为唯一就绪作业被派发（退避语义不变），并阻塞占住并发槽
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            with lock:
                if "low" in started:
                    break
            time.sleep(0.01)
        with lock:
            assert started.index("flaky") < started.index("low")
        # low 占槽期间再来一个 normal 作业
        svc.submit("job-normal", "normal", priority="normal")
        time.sleep(0.2)  # 等 flaky 的退避（0.1s）确定到期
        low_release.set()
        wait_status(svc, "job-flaky", JobStatus.SUCCEEDED)
        wait_status(svc, "job-normal", JobStatus.SUCCEEDED)
        job = svc.get("job-flaky")
        assert job.attempts == 2  # 失败一次后重试成功
        assert job.priority == 10  # 重试不丢失静态优先级
        assert any("RUNNING->QUEUED retry" in h for h in job.history)
        with lock:
            # 退避到期后，重试的 flaky（高优先级）排在 normal 之前重新派发
            assert started == ["blocker", "flaky", "low", "flaky", "normal"]
        logger.info("retry requeue order: %s", started)
    finally:
        blocker_release.set()
        low_release.set()
        svc.stop()


def test_duplicate_submit_with_priority_is_idempotent():
    """带优先级的重复提交仍幂等：返回首次提交的作业，优先级不被覆盖。"""
    svc = make_service(lambda p: None)
    try:
        job1, created1 = svc.submit("job-dup", "p", priority="high")
        job2, created2 = svc.submit("job-dup", "p", priority="low")
        assert created1 is True
        assert created2 is False
        assert job1.priority == 10
        assert job2.priority == 10  # 保留首次提交的优先级
        final = wait_status(svc, "job-dup", JobStatus.SUCCEEDED)
        assert final.attempts == 1
        logger.info("duplicate submit keeps original priority=%d", final.priority)
    finally:
        svc.stop()
