from __future__ import annotations

import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest import mock


SCRIPT = Path(__file__).parents[1] / "m4-local-demo.py"
SPEC = importlib.util.spec_from_file_location("m4_local_demo", SCRIPT)
assert SPEC and SPEC.loader
DEMO = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(DEMO)


class M4LocalDemoTest(unittest.TestCase):
    def test_output_is_private_new_and_outside_live_packet(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            local = root / ".evidence" / "m4-local"
            with (
                mock.patch.object(DEMO, "ROOT", root),
                mock.patch.object(DEMO, "LOCAL_EVIDENCE_ROOT", local),
            ):
                output = DEMO.validate_output(
                    Path(".evidence/m4-local/20260908T013000Z/demo-rehearsal.json")
                )
                self.assertEqual(output.parent.stat().st_mode & 0o777, 0o700)
                DEMO.write_file(output, "{}\n")
                self.assertEqual(output.stat().st_mode & 0o777, 0o600)

                with self.assertRaisesRegex(DEMO.DemoError, "already exists"):
                    DEMO.validate_output(output)
                with self.assertRaisesRegex(DEMO.DemoError, "beneath"):
                    DEMO.validate_output(root / ".evidence" / "m4" / "live.json")

    def test_provenance_checks_every_running_workload_image(self):
        pods = {
            "relay-ingest": [self.pod("ingest-a"), self.pod("ingest-b")],
            "relay-deliver": [self.pod("deliver-a"), self.pod("deliver-b")],
            "sink": [self.pod("sink-a")],
        }
        sigterm = mock.Mock()
        sigterm.ready_pods.side_effect = lambda application: pods[application]
        sigterm.image_provenance.return_value = {
            "image_id": "docker://sha256:" + "b" * 64,
            "image_revision": "a" * 7,
        }

        result = DEMO.collect_provenance(sigterm, "a" * 40)

        self.assertEqual(sigterm.image_provenance.call_count, 5)
        self.assertEqual(len(result["relay-ingest"]), 2)
        self.assertEqual(len(result["relay-deliver"]), 2)
        self.assertEqual(len(result["sink"]), 1)

    def test_provenance_rejects_wrong_fixed_replica_count(self):
        sigterm = mock.Mock()
        sigterm.ready_pods.return_value = [self.pod("only-one")]

        with self.assertRaisesRegex(DEMO.DemoError, "want 2"):
            DEMO.collect_provenance(sigterm, "a" * 40)

    def test_summary_records_observed_scale_dlq_and_replay(self):
        transcript = """
{"id":"evt_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
t=0s 0 1
t=12s 596 3
t=30s 0 1
{"id":"evt_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}
delivered=40 dead-lettered=7
delivered=41 dead-lettered=8
replaying. every event after that point is being delivered again
"""

        self.assertEqual(
            DEMO.summarize_demo(transcript),
            {
                "event_ids": [
                    "evt_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "evt_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                ],
                "scale_samples": 3,
                "peak_lag": 596,
                "peak_consumers": 3,
                "final_lag": 0,
                "final_consumers": 1,
                "healthy_delivery_counter_delta": 1,
                "dead_letter_counter_delta": 1,
                "replay_completed": True,
            },
        )

    def test_summary_rejects_a_preexisting_dlq_counter(self):
        transcript = """
{"id":"evt_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
{"id":"evt_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}
t=0s 0 1
delivered=40 dead-lettered=7
delivered=41 dead-lettered=7
replaying. every event after that point is being delivered again
"""

        with self.assertRaisesRegex(DEMO.DemoError, "fresh dead-letter"):
            DEMO.summarize_demo(transcript)

    @staticmethod
    def pod(name: str) -> dict[str, object]:
        return {"metadata": {"name": name, "uid": name + "-uid"}}


if __name__ == "__main__":
    unittest.main()
