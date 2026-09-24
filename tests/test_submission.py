"""幂等提交、进程中断恢复与非法流转拒绝测试。"""

import logging
import tempfile
import unittest
from pathlib import Path

from orchestrator.errors import InvalidStateTransitionError, JobNotFoundError
from orchestrator.handlers import HandlerRegistry
from orchestrator.models import Job
from orchestrator.scheduler import Scheduler
from orchestrator.states import JobState
from orchestrator.store import JsonJobStore

logging.basicConfig(level=logging.INFO,
                    format="%(levelname)s [%(name)s] %(message)s")


class SubmissionTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self._tmp.name)
        self.reg = HandlerRegistry()
        self.reg.register("ok", lambda p: {"v": p})

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def make(self, name: str = "jobs.json", **kw) -> Scheduler:
        return Scheduler(JsonJobStore(self.dir / name), self.reg,
                         base_backoff=0.01, poll_interval=0.01, **kw)

    def test_duplicate_submit_is_idempotent(self) -> None:
        sch = self.make()
        first = sch.submit("dup-1", "ok", {"x": 1})
        second = sch.submit("dup-1", "ok", {"x": 2})
        self.assertTrue(first.created)
        self.assertFalse(second.created)
        self.assertIs(first.job, second.job)
        # 原 payload 与排队序号不被重复提交改变
        self.assertEqual(second.job.payload, {"x": 1})
        self.assertEqual(second.job.submit_seq, 1)
        kinds = [e.kind for e in second.job.events]
        self.assertIn("submitted", kinds)
        self.assertIn("duplicate_submit", kinds)
        logging.info("job=dup-1 duplicate submit converged, seq=%d state=%s",
                     second.job.submit_seq, second.job.state.value)

    def test_get_unknown_job_raises(self) -> None:
        sch = self.make()
        with self.assertRaises(JobNotFoundError):
            sch.get("missing")

    def test_invalid_transition_raises_distinct_error(self) -> None:
        sch = self.make()
        sch.submit("t1", "ok")
        job = sch.get("t1")
        # pending -> succeeded 不存在
        with self.assertRaises(InvalidStateTransitionError) as cm:
            sch.transition(job, JobState.SUCCEEDED)
        self.assertEqual(cm.exception.reason, "illegal_transition")
        self.assertEqual(cm.exception.job_id, "t1")

    def test_interrupted_running_job_recovers_to_pending(self) -> None:
        path = self.dir / "jobs.json"
        store = JsonJobStore(path)
        sch = Scheduler(store, self.reg, base_backoff=0.01)
        sch.submit("interrupted-1", "ok")
        sch.submit("interrupted-2", "ok")
        # 手工把作业置为 RUNNING，模拟进程在执行中被杀
        j1 = sch.get("interrupted-1")
        j1.state = JobState.RUNNING
        j1.attempts = 1
        store.put(j1)

        # 新进程：重新构建 store 与 scheduler 并 recover
        store2 = JsonJobStore(path)
        sch2 = Scheduler(store2, self.reg, base_backoff=0.01,
                         poll_interval=0.01)
        n = sch2.recover()
        self.assertEqual(n, 1)
        recovered = sch2.get("interrupted-1")
        self.assertIs(recovered.state, JobState.PENDING)
        self.assertEqual(recovered.attempts, 1)  # 尝试次数保留
        reason = recovered.events[-1].reason
        self.assertEqual(reason, "interrupted_recovery")
        logging.info(
            "job=interrupted-1 recovered running -> pending (reason=%s)",
            reason)

        # 恢复后可正常被调度执行成功，收敛为终态
        sch2.start()
        self.assertTrue(sch2.wait_idle(5))
        sch2.stop()
        self.assertIs(sch2.get("interrupted-1").state, JobState.SUCCEEDED)

    def test_persistence_survives_reconstruction(self) -> None:
        path = self.dir / "p.json"
        sch = Scheduler(JsonJobStore(path), self.reg)
        sch.submit("persist-1", "ok", {"k": "v"})
        # 不经过调度器，直接从磁盘重建
        rebuilt = JsonJobStore(path).get("persist-1")
        self.assertIsInstance(rebuilt, Job)
        assert rebuilt is not None
        self.assertEqual(rebuilt.job_type, "ok")
        self.assertEqual(rebuilt.payload, {"k": "v"})


if __name__ == "__main__":
    unittest.main()
