"""本地持久化仓库测试：原子写、排序、进程内并发安全。"""

import json
import tempfile
import threading
import unittest
from pathlib import Path

from orchestrator.models import Job
from orchestrator.states import JobState
from orchestrator.store import JsonJobStore


class JsonJobStoreTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.path = Path(self._tmp.name) / "jobs.json"

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_roundtrip_and_ordering(self) -> None:
        store = JsonJobStore(self.path)
        store.put(Job(job_id="z", job_type="t", payload={}, submit_seq=2))
        store.put(Job(job_id="a", job_type="t", payload={}, submit_seq=1))
        reloaded = JsonJobStore(self.path)
        self.assertEqual([j.job_id for j in reloaded.all()], ["a", "z"])

    def test_atomic_write_leaves_no_temp_file(self) -> None:
        store = JsonJobStore(self.path)
        store.put(Job(job_id="x", job_type="t", payload={}))
        leftovers = list(self.path.parent.glob("*.tmp"))
        self.assertEqual(leftovers, [])
        # 文件内容为合法 JSON
        json.loads(self.path.read_text(encoding="utf-8"))

    def test_reload_preserves_state_and_events(self) -> None:
        store = JsonJobStore(self.path)
        j = Job(job_id="e", job_type="t", payload={"k": 1},
                state=JobState.WAITING_RETRY, attempts=2, run_after=123.5)
        store.put(j)
        got = JsonJobStore(self.path).get("e")
        self.assertIsNotNone(got)
        assert got is not None
        self.assertIs(got.state, JobState.WAITING_RETRY)
        self.assertEqual(got.attempts, 2)
        self.assertEqual(got.run_after, 123.5)

    def test_concurrent_puts_lose_nothing(self) -> None:
        store = JsonJobStore(self.path)

        def worker(base: int) -> None:
            for i in range(base, base + 25):
                store.put(Job(job_id=f"j{i}", job_type="t", payload={},
                              submit_seq=i))

        threads = [threading.Thread(target=worker, args=(b,))
                   for b in (0, 25, 50, 75)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(len(store.all()), 100)


if __name__ == "__main__":
    unittest.main()
