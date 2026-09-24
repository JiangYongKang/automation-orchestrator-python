"""本地 JSON 文件持久化（单文件原子写 + 进程内锁）。"""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path

from orchestrator.models import Job


class JsonJobStore:
    """作业仓库。

    - 进程内所有读写经同一把可重入锁串行化（调度器线程 / API 线程并发安全）。
    - 落盘采用「写临时文件 + fsync + os.replace 原子替换」，进程被中断时
      不会读到半截文件。
    """

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self._lock = threading.RLock()
        self._jobs: dict[str, Job] = {}
        if self.path.exists():
            self.load()

    def put(self, job: Job) -> None:
        """新增或整体覆盖一个作业（以 job_id 为主键）并落盘。"""
        with self._lock:
            self._jobs[job.job_id] = job
            self.flush()

    def get(self, job_id: str) -> Job | None:
        with self._lock:
            return self._jobs.get(job_id)

    def all(self) -> list[Job]:
        """返回全部作业，按提交序号排序（同序号按 job_id 稳定排序）。"""
        with self._lock:
            return sorted(self._jobs.values(),
                          key=lambda j: (j.submit_seq, j.job_id))

    def delete(self, job_id: str) -> bool:
        with self._lock:
            existed = self._jobs.pop(job_id, None) is not None
            if existed:
                self.flush()
            return existed

    def load(self) -> None:
        """从磁盘加载（启动恢复用）。文件缺失视为空仓库。"""
        with self._lock:
            if not self.path.exists():
                self._jobs = {}
                return
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            self._jobs = {
                item["job_id"]: Job.from_dict(item) for item in raw["jobs"]
            }

    def flush(self) -> None:
        """显式落盘（原子替换）。"""
        with self._lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            payload = json.dumps(
                {"jobs": [j.to_dict() for j in self.all()]},
                ensure_ascii=False, indent=2,
            )
            tmp = self.path.with_suffix(self.path.suffix + ".tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(payload)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.path)
