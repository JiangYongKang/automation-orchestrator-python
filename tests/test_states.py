"""状态模型与非法流转原因码测试。

单测日志要求：打印作业标识、状态迁移与失败分类。
"""

import logging
import unittest

from orchestrator.states import (
    REASON_ALREADY_IN_STATE,
    REASON_ILLEGAL_TRANSITION,
    REASON_TERMINAL_STATE,
    TERMINAL_STATES,
    JobState,
    can_transition,
    reject_reason,
)

logging.basicConfig(level=logging.DEBUG,
                    format="%(levelname)s [%(name)s] %(message)s")


class StateModelTest(unittest.TestCase):
    def test_legal_paths(self) -> None:
        cases = [
            (JobState.PENDING, JobState.RUNNING),
            (JobState.RUNNING, JobState.SUCCEEDED),
            (JobState.RUNNING, JobState.FAILED),
            (JobState.RUNNING, JobState.WAITING_RETRY),
            (JobState.WAITING_RETRY, JobState.PENDING),
        ]
        for src, dst in cases:
            with self.subTest(src=src, dst=dst):
                self.assertIsNone(reject_reason(src, dst))
                self.assertTrue(can_transition(src, dst))
                logging.info("legal transition %s -> %s", src.value, dst.value)

    def test_completed_cannot_run_again_is_terminal_reason(self) -> None:
        # 已完成作业被重新置为运行中 -> 必须拒绝且原因可区分
        for terminal in TERMINAL_STATES:
            with self.subTest(terminal=terminal):
                self.assertEqual(
                    reject_reason(terminal, JobState.RUNNING),
                    REASON_TERMINAL_STATE,
                )
                logging.info("job=<terminal-%s> rejected: %s",
                             terminal.value, REASON_TERMINAL_STATE)

    def test_same_state_is_distinct_reason(self) -> None:
        self.assertEqual(
            reject_reason(JobState.PENDING, JobState.PENDING),
            REASON_ALREADY_IN_STATE,
        )

    def test_nonexistent_path_is_illegal_reason(self) -> None:
        # pending 不能直接成功；waiting_retry 不能直接成功
        self.assertEqual(
            reject_reason(JobState.PENDING, JobState.SUCCEEDED),
            REASON_ILLEGAL_TRANSITION,
        )
        self.assertEqual(
            reject_reason(JobState.WAITING_RETRY, JobState.RUNNING),
            REASON_ILLEGAL_TRANSITION,
        )


if __name__ == "__main__":
    unittest.main()
