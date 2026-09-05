import importlib.util
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

MODULE_PATH = Path(__file__).with_name("main.py")
SPEC = importlib.util.spec_from_file_location("iterate_watchdog", MODULE_PATH)
watchdog = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = watchdog
SPEC.loader.exec_module(watchdog)


class ConversationClassificationTests(unittest.TestCase):
    def test_active_recent_conversation_remains_open(self):
        now = datetime(2026, 9, 5, 12, tzinfo=timezone.utc)
        conversation = {
            "status": "running",
            "updated_at": (now - timedelta(minutes=30)).isoformat(),
        }
        self.assertEqual(watchdog.effective_conversation_status(conversation, now), "running")

    def test_active_stale_conversation_becomes_stuck(self):
        now = datetime(2026, 9, 5, 12, tzinfo=timezone.utc)
        conversation = {
            "status": "running",
            "updated_at": (now - watchdog.CONVERSATION_LEASE - timedelta(seconds=1)).isoformat(),
        }
        self.assertEqual(watchdog.effective_conversation_status(conversation, now), "stuck")

    def test_finished_conversation_remains_finished(self):
        now = datetime(2026, 9, 5, 12, tzinfo=timezone.utc)
        conversation = {
            "status": "finished",
            "updated_at": (now - timedelta(days=2)).isoformat(),
        }
        self.assertEqual(watchdog.effective_conversation_status(conversation, now), "finished")


class HumanMarkerTests(unittest.TestCase):
    def test_machine_blocker_invalidates_human_pause(self):
        reasons = ["PR has merge conflicts with its base branch", "review still requested from bot"]
        self.assertFalse(watchdog.should_pause_for_human("needs human approval", reasons))

    def test_human_only_gate_can_pause(self):
        reasons = ["review still requested from all-hands-bot"]
        self.assertTrue(watchdog.should_pause_for_human("needs human approval", reasons))

    def test_documented_human_description_failure_can_pause(self):
        reasons = ["CI failing (1/7 current workflows: PR Description Check)"]
        marker = "The HUMAN: section needs genuine human-written evidence. needs human approval"
        self.assertTrue(watchdog.should_pause_for_human(marker, reasons))

    def test_unrelated_ci_failure_invalidates_description_marker(self):
        reasons = ["CI failing (1/7 current workflows: test)"]
        marker = "The HUMAN: section needs genuine human-written evidence. needs human approval"
        self.assertFalse(watchdog.should_pause_for_human(marker, reasons))

    def test_mixed_description_and_title_failures_invalidate_marker(self):
        reasons = [
            "CI failing (2/7 current workflows: Validate PR description, pr-title / Lint PR title (conventional))"
        ]
        marker = "The HUMAN: section needs human-written evidence. needs human approval"
        self.assertFalse(watchdog.should_pause_for_human(marker, reasons))

    def test_missing_agent_owned_template_sections_invalidate_marker(self):
        reasons = ["CI failing (1/7 current workflows: Validate PR description)"]
        marker = "HUMAN text and missing template sections are required. needs human approval"
        self.assertFalse(watchdog.should_pause_for_human(marker, reasons))

    def test_no_marker_never_pauses(self):
        self.assertFalse(watchdog.should_pause_for_human(None, ["review still requested from bot"]))


class SchedulingTests(unittest.TestCase):
    def test_never_dispatched_precedes_oldest_dispatched(self):
        candidates = [
            ({"full_name": "o/r", "number": 1, "updated_at": "2026-09-05T12:00:00Z"}, []),
            ({"full_name": "o/r", "number": 2, "updated_at": "2026-09-01T12:00:00Z"}, []),
            ({"full_name": "o/r", "number": 3, "updated_at": "2026-09-04T12:00:00Z"}, []),
        ]
        state = {
            "o/r#1": {"last_dispatched_at": "2026-09-05T11:00:00Z"},
            "o/r#2": {"last_dispatched_at": "2026-09-02T11:00:00Z"},
        }
        ordered = sorted(candidates, key=lambda item: watchdog.candidate_priority(item[0], state))
        self.assertEqual([item[0]["number"] for item in ordered], [3, 2, 1])

    def test_capacity_blocks_followups_too(self):
        self.assertFalse(watchdog.has_engagement_capacity(8, 8, 0, 4))
        self.assertFalse(watchdog.has_engagement_capacity(7, 8, 4, 4))
        self.assertTrue(watchdog.has_engagement_capacity(7, 8, 3, 4))


if __name__ == "__main__":
    unittest.main()
