"""本地持久化层：基于 sqlite3 的作业存储，支持原子状态迁移与崩溃恢复。"""
from __future__ import annotations

import json
import sqlite3
import threading
import time

from .models import Job, JobStatus

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,
    payload TEXT NOT NULL,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL,
    max_retries INTEGER NOT NULL,
    priority INTEGER NOT NULL DEFAULT 5,
    failure_class TEXT,
    conclusion TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    queued_at REAL NOT NULL DEFAULT 0,
    not_before REAL NOT NULL,
    history TEXT NOT NULL
)
"""

# 老版本库缺少的列：(列名, DDL)，启动时按需 ALTER TABLE 补齐
_MIGRATIONS: tuple[tuple[str, str], ...] = (
    ("priority", "ALTER TABLE jobs ADD COLUMN priority INTEGER NOT NULL DEFAULT 5"),
    ("queued_at", "ALTER TABLE jobs ADD COLUMN queued_at REAL NOT NULL DEFAULT 0"),
)


class JobStore:
    """基于 SQLite 的作业持久化，进程内多线程共享一把锁保证串行化。"""

    def __init__(self, db_path: str = ":memory:") -> None:
        self._db_path = db_path
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute(_SCHEMA)
            self._migrate()
            self._conn.commit()

    def _migrate(self) -> None:
        """对老版本库按需补列（幂等）。"""
        cols = {r["name"] for r in self._conn.execute("PRAGMA table_info(jobs)")}
        for name, ddl in _MIGRATIONS:
            if name not in cols:
                self._conn.execute(ddl)

    def save(self, job: Job) -> None:
        """插入或更新作业记录（按 job_id 覆盖）。"""
        with self._lock:
            self._conn.execute(
                """INSERT OR REPLACE INTO jobs
                   (job_id, payload, status, attempts, max_retries, priority, failure_class,
                    conclusion, created_at, updated_at, queued_at, not_before, history)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    job.job_id, job.payload, job.status.value, job.attempts,
                    job.max_retries, job.priority, job.failure_class, job.conclusion,
                    job.created_at, job.updated_at, job.queued_at, job.not_before,
                    json.dumps(job.history),
                ),
            )
            self._conn.commit()

    def get(self, job_id: str) -> Job | None:
        """按 ID 读取作业，不存在返回 None。"""
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM jobs WHERE job_id = ?", (job_id,)
            ).fetchone()
        return self._to_job(row) if row else None

    def compare_and_swap_status(
        self, job_id: str, expected: JobStatus, target: JobStatus, **fields: object
    ) -> bool:
        """原子状态迁移：当前状态等于 expected 才更新为 target。

        并发争用时只有一个调用方能成功，返回是否抢到。
        """
        sets = ["status = ?"]
        params: list[object] = [target.value]
        for key, value in fields.items():
            if key == "history":
                value = json.dumps(value)
            sets.append(f"{key} = ?")
            params.append(value)
        params.extend([job_id, expected.value])
        with self._lock:
            cur = self._conn.execute(
                f"UPDATE jobs SET {', '.join(sets)} WHERE job_id = ? AND status = ?",
                params,
            )
            self._conn.commit()
            return cur.rowcount == 1

    def list_by_status(self, status: JobStatus) -> list[Job]:
        """按状态列出作业（按创建时间升序，保证 FIFO）。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM jobs WHERE status = ? ORDER BY created_at, job_id",
                (status.value,),
            ).fetchall()
        return [self._to_job(r) for r in rows]

    def recover_interrupted(self) -> list[str]:
        """把崩溃时停留在 RUNNING 的作业重置为 QUEUED，返回受影响的 job_id。

        恢复时把 queued_at 重置为当前时刻：防饿死计时从恢复后重新起算，
        崩溃前已等待的时长不计入（崩溃期间系统不在正常运转）。
        """
        with self._lock:
            rows = self._conn.execute(
                "SELECT job_id FROM jobs WHERE status = ?", (JobStatus.RUNNING.value,)
            ).fetchall()
            ids = [r["job_id"] for r in rows]
            self._conn.execute(
                "UPDATE jobs SET status = ?, queued_at = ? WHERE status = ?",
                (JobStatus.QUEUED.value, time.time(), JobStatus.RUNNING.value),
            )
            self._conn.commit()
        return ids

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    @staticmethod
    def _to_job(row: sqlite3.Row) -> Job:
        return Job(
            job_id=row["job_id"],
            payload=row["payload"],
            status=JobStatus(row["status"]),
            attempts=row["attempts"],
            max_retries=row["max_retries"],
            priority=row["priority"],
            failure_class=row["failure_class"],
            conclusion=row["conclusion"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            queued_at=row["queued_at"],
            not_before=row["not_before"],
            history=json.loads(row["history"]),
        )
