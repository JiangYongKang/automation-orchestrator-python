"""失败分类、重试退避、重试耗尽结论与作业隔离测试。"""

import logging
import tempfile
import unittest
from pathlib import Path

from orchestrator.errors import FatalJobError, RetryableJobError
from orchestrator.handlers import HandlerRegistry
from orchestrator.retry import backoff_delay, classify_error
from orchestrator.scheduler import Scheduler
from orchestrator.states import JobState
from orchestrator.store import JsonJobStore

logging.basicConfig(level=logging.INFO,
                    format="%(levelname)s [%(name)s] %(message)s")


class FailureClassificationTest(unittest.TestCase):
    def test_classify(self) -> None:
        self.assertEqual(classify_error(RetryableJobError("t")), "retryable")
        self.assertEqual(classify_error(FatalJobError("f")), "fatal")
        # 未知异常保守按可重试处理
        self.assertEqual(classify_error(RuntimeError("?")), "retryable")

    def test_backoff_is_exponential_and_capped(self) -> None:
        delays = [round(backoff_delay(a, 0.01, 2.0, 0.1), 4)
                  for a in range(1, 6)]
        self.assertEqual(delays, [0.01, 0.02, 0.04, 0.08, 0.1])  # 末项触顶
        logging.info("retry backoff sequence: %s", delays)


class RetryExecutionTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self._tmp.name)
        self.reg = HandlerRegistry()

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def make(self, **kw) -> Scheduler:
        kw.setdefault("poll_interval", 0.01)
        kw.setdefault("base_backoff", 0.01)
        return Scheduler(
            JsonJobStore(self.dir / f"j{id(self)}.json"), self.reg, **kw)

    def test_retry_then_success_records_attempts(self) -> None:
        state = {"calls": 0}

        def flaky(_p):
            state["calls"] += 1
            if state["calls"] < 3:
                raise RetryableJobError("temporary blip")
            return {"repaired": True}

        self.reg.register("flaky", flaky)
        sch = self.make(max_concurrency=1)
        sch.submit("retry-job", "flaky")
        sch.start()
        self.assertTrue(sch.wait_idle(5))
        sch.stop()

        job = sch.get("retry-job")
        self.assertIs(job.state, JobState.SUCCEEDED)
        self.assertEqual(job.attempts, 3)
        self.assertEqual(job.result, {"repaired": True})
        retries = [e for e in job.events if e.kind == "retry_scheduled"]
        self.assertEqual(len(retries), 2)
        classes = [e.error_class for e in job.events
                   if e.kind == "attempt_failed"]
        self.assertEqual(classes, ["retryable", "retryable"])
        logging.info(
            "job=retry-job converged to succeeded after %d attempts "
            "(error_classes=%s)", job.attempts, classes)

    def test_retries_exhausted_gives_trackable_conclusion(self) -> None:
        def always_bad(_p):
            raise RetryableJobError("still broken")

        self.reg.register("bad", always_bad)
        sch = self.make(max_concurrency=1, default_max_attempts=3)
        sch.submit("exhaust-job", "bad")
        sch.start()
        self.assertTrue(sch.wait_idle(5))
        sch.stop()

        job = sch.get("exhaust-job")
        self.assertIs(job.state, JobState.FAILED)
        self.assertEqual(job.attempts, 3)
        self.assertEqual(job.failure_reason, "retries_exhausted")
        self.assertIsNotNone(job.failure_message)
        self.assertIsNotNone(job.finished_at)
        logging.info(
            "job=exhaust-job FAILED reason=retries_exhausted attempts=%d/%d",
            job.attempts, job.max_attempts)

    def test_fatal_error_fails_immediately_without_retry(self) -> None:
        state = {"calls": 0}

        def fatal(_p):
            state["calls"] += 1
            raise FatalJobError("invalid parameter")

        self.reg.register("fatal-job", fatal)
        sch = self.make(max_concurrency=1, default_max_attempts=5)
        sch.submit("fatal-1", "fatal-job")
        sch.start()
        self.assertTrue(sch.wait_idle(5))
        sch.stop()

        job = sch.get("fatal-1")
        self.assertIs(job.state, JobState.FAILED)
        self.assertEqual(job.attempts, 1)          # 不重试
        self.assertEqual(job.failure_reason, "fatal_error")
        self.assertEqual(state["calls"], 1)
        logging.info("job=fatal-1 FAILED reason=fatal_error after 1 attempt")

    def test_unknown_exception_is_retried_then_exhausts(self) -> None:
        state = {"calls": 0}

        def boom(_p):
            state["calls"] += 1
            raise ValueError("unexpected!")  # 未分类异常 -> 保守可重试

        self.reg.register("boom", boom)
        sch = self.make(max_concurrency=1, default_max_attempts=2)
        sch.submit("boom-1", "boom")
        sch.start()
        self.assertTrue(sch.wait_idle(5))
        sch.stop()
        job = sch.get("boom-1")
        self.assertIs(job.state, JobState.FAILED)
        self.assertEqual(job.attempts, 2)
        self.assertEqual(job.failure_reason, "retries_exhausted")
        logging.info(
            "job=boom-1 unknown error retried then exhausted (attempts=%d)",
            job.attempts)

    def test_missing_handler_is_fatal_not_retryable(self) -> None:
        sch = self.make(max_concurrency=1)
        sch.submit("nohandler", "unregistered-type")
        sch.start()
        self.assertTrue(sch.wait_idle(5))
        sch.stop()
        job = sch.get("nohandler")
        self.assertIs(job.state, JobState.FAILED)
        self.assertEqual(job.failure_reason, "handler_not_found")
        self.assertEqual(job.attempts, 1)
        logging.info("job=nohandler FAILED reason=handler_not_found")

    def test_one_job_failure_does_not_block_others(self) -> None:
        """单个作业失败不得影响其它作业，调度器持续可用。"""
        self.reg.register("good", lambda _p: {"ok": True})

        def failing(_p):
            raise FatalJobError("permanent")

        self.reg.register("failing", failing)
        sch = self.make(max_concurrency=2, default_max_attempts=1)

        # 交错提交：失败作业夹在成功作业之间
        sch.submit("g1", "good")
        sch.submit("bad1", "failing")
        sch.submit("g2", "good")
        sch.submit("bad2", "failing")
        sch.submit("g3", "good")
        sch.start()
        self.assertTrue(sch.wait_idle(5))
        sch.stop()

        for gid in ("g1", "g2", "g3"):
            self.assertIs(sch.get(gid).state, JobState.SUCCEEDED, gid)
        for bid in ("bad1", "bad2"):
            j = sch.get(bid)
            self.assertIs(j.state, JobState.FAILED, bid)
            self.assertEqual(j.failure_reason, "fatal_error")
        logging.info(
            "isolation check: good jobs=%s failed jobs=%s",
            ["g1", "g2", "g3"], ["bad1", "bad2"])


if __name__ == "__main__":
    unittest.main()
