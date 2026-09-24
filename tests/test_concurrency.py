"""并发上限、FIFO 排队顺序与维护窗口规则测试。"""

import logging
import tempfile
import threading
import time
import unittest
from pathlib import Path

from orchestrator.handlers import HandlerRegistry
from orchestrator.scheduler import Scheduler
from orchestrator.states import JobState
from orchestrator.store import JsonJobStore

logging.basicConfig(level=logging.INFO,
                    format="%(levelname)s [%(name)s] %(message)s")


class ConcurrencyTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def make(self, reg: HandlerRegistry, **kw) -> Scheduler:
        kw.setdefault("poll_interval", 0.01)
        return Scheduler(JsonJobStore(self.dir / "jobs.json"), reg, **kw)

    def test_over_limit_jobs_queue_fifo_not_rejected(self) -> None:
        """超过并发上限进入队列（仍是 PENDING），按提交顺序 FIFO 启动。"""
        reg = HandlerRegistry()
        order: list[str] = []
        gate = threading.Event()

        def slow(job_alias: str):
            def _h(_p):
                order.append(job_alias)
                gate.wait(timeout=2)
                return {"done": job_alias}
            return _h

        for alias in ("a", "b", "c", "d"):
            reg.register(f"job-{alias}", slow(alias))

        sch = self.make(reg, max_concurrency=2, base_backoff=0.01)
        for alias in ("a", "b", "c", "d"):
            sch.submit(f"id-{alias}", f"job-{alias}")
        sch.run_once()  # 只扫描一次：应恰好启动 2 个，其余排队

        self.assertEqual(sch.active_count, 2)
        states = {a: sch.get(f"id-{a}").state for a in "abcd"}
        self.assertIs(states["a"], JobState.RUNNING)
        self.assertIs(states["b"], JobState.RUNNING)
        self.assertIs(states["c"], JobState.PENDING)  # 排队而非拒绝
        self.assertIs(states["d"], JobState.PENDING)
        logging.info("queue state after dispatch: %s",
                     {k: v.value for k, v in states.items()})

        gate.set()
        sch.start()
        self.assertTrue(sch.wait_idle(5))
        sch.stop()
        # FIFO：前两个先执行，后两个后执行
        self.assertEqual(order[:2].count("a") + order[:2].count("b"), 2)
        self.assertEqual(order[2:], ["c", "d"])
        for alias in "abcd":
            self.assertIs(sch.get(f"id-{alias}").state, JobState.SUCCEEDED)
        logging.info("FIFO execution order: %s", order)

    def test_maintenance_window_blocks_new_starts_drains_running(self) -> None:
        """窗口内不启动新作业；已运行作业跑到自然终态；窗口结束后队列恢复。"""
        reg = HandlerRegistry()
        running_started = threading.Event()
        release = threading.Event()

        def slow_ok(_p):
            running_started.set()
            release.wait(timeout=2)
            return {"ok": True}

        reg.register("slow", slow_ok)
        reg.register("fast", lambda _p: {"ok": True})

        sch = self.make(reg, max_concurrency=1, base_backoff=0.01)
        sch.submit("run-1", "slow")
        sch.submit("wait-1", "fast")

        sch.run_once()  # run-1 启动；wait-1 排队
        self.assertTrue(running_started.wait(1))
        sch.begin_maintenance()
        self.assertTrue(sch.in_maintenance)

        # 多次扫描，wait-1 在窗口内必须保持 PENDING
        for _ in range(5):
            self.assertEqual(sch.run_once(), 0)
        self.assertIs(sch.get("wait-1").state, JobState.PENDING)
        logging.info("job=wait-1 held in queue during maintenance (pending)")

        # drain：run-1 不被强杀，放它自然跑完 -> SUCCEEDED
        release.set()
        deadline = time.time() + 3
        while time.time() < deadline and sch.get("run-1").state is not JobState.SUCCEEDED:
            time.sleep(0.02)
        self.assertIs(sch.get("run-1").state, JobState.SUCCEEDED)
        logging.info("job=run-1 drained to natural terminal state=succeeded")
        # wait-1 窗口内仍不得启动
        self.assertIs(sch.get("wait-1").state, JobState.PENDING)

        sch.end_maintenance()
        sch.start()
        self.assertTrue(sch.wait_idle(5))
        sch.stop()
        self.assertIs(sch.get("wait-1").state, JobState.SUCCEEDED)
        logging.info("job=wait-1 started and succeeded after maintenance")

    def test_concurrent_submissions_do_not_corrupt_state(self) -> None:
        """多线程并发提交不同作业：全部唯一入队、序号连续，无丢失。"""
        reg = HandlerRegistry()
        reg.register("noop", lambda _p: {"ok": True})
        sch = self.make(reg, max_concurrency=4)

        def submit(i: int) -> None:
            sch.submit(f"c-{i:03d}", "noop")

        threads = [threading.Thread(target=submit, args=(i,))
                   for i in range(50)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        seqs = sorted(sch.get(f"c-{i:03d}").submit_seq for i in range(50))
        self.assertEqual(seqs, list(range(1, 51)))
        sch.start()
        self.assertTrue(sch.wait_idle(5))
        sch.stop()
        for i in range(50):
            self.assertIs(sch.get(f"c-{i:03d}").state, JobState.SUCCEEDED)
        logging.info("50 concurrent submissions all converged to succeeded")


if __name__ == "__main__":
    unittest.main()
