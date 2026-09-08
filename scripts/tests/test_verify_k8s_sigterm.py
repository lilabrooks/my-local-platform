from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock


SCRIPT = Path(__file__).parents[1] / "verify-k8s-sigterm.py"
SPEC = importlib.util.spec_from_file_location("verify_k8s_sigterm", SCRIPT)
assert SPEC and SPEC.loader
VERIFY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(VERIFY)


class K8sSigtermTest(unittest.TestCase):
    def events(
        self,
        *,
        ready_at: float = 11.0,
        exit_at: float = 12.0,
        exit_code: str = "0",
        signal: str = "0",
    ) -> list[dict[str, object]]:
        return [
            {
                "observed_at": 10.0,
                "deletion_timestamp": "<no value>",
                "ready": "True",
                "exit_code": "<no value>",
                "signal": "<no value>",
                "reason": "<no value>",
            },
            {
                "observed_at": ready_at,
                "deletion_timestamp": "2026-09-07T22:00:00Z",
                "ready": "False",
                "exit_code": "<no value>",
                "signal": "<no value>",
                "reason": "<no value>",
            },
            {
                "observed_at": exit_at,
                "deletion_timestamp": "2026-09-07T22:00:00Z",
                "ready": "False",
                "exit_code": exit_code,
                "signal": signal,
                "reason": "Completed" if exit_code == "0" else "Error",
            },
        ]

    def test_termination_requires_readiness_then_clean_exit_inside_grace(self):
        result = VERIFY.analyze_termination(self.events(), 10.5, 11.0, 45)

        self.assertTrue(result["readiness_false_before_exit"])
        self.assertEqual(result["exit_code"], 0)
        self.assertEqual(result["signal"], 0)
        self.assertEqual(result["elapsed_seconds"], 1.5)

        omitted_signal = VERIFY.analyze_termination(
            self.events(signal="<no value>"), 10.5, 11.0, 45
        )
        self.assertEqual(omitted_signal["signal"], 0)

    def test_termination_rejects_simultaneous_readiness_and_exit(self):
        with self.assertRaisesRegex(VERIFY.VerificationError, "before readiness"):
            VERIFY.analyze_termination(self.events(ready_at=12.0), 10.5, 12.0, 45)

    def test_termination_rejects_sigkill_and_grace_overrun(self):
        with self.assertRaisesRegex(VERIFY.VerificationError, "signal 9"):
            VERIFY.analyze_termination(
                self.events(exit_code="137", signal="9"), 10.5, 11.0, 45
            )

        with self.assertRaisesRegex(VERIFY.VerificationError, "outside"):
            VERIFY.analyze_termination(self.events(exit_at=56.0), 10.5, 11.0, 45)

    def test_delivery_result_accepts_completion_or_safe_redelivery(self):
        outcomes = ["delivered", "retrying", "exhausted"]

        self.assertEqual(VERIFY.delivery_result(outcomes, 1), "completed")
        self.assertEqual(VERIFY.delivery_result(outcomes, 2), "redelivered")
        with self.assertRaises(VERIFY.VerificationError):
            VERIFY.delivery_result(["delivered"], 1)
        with self.assertRaises(VERIFY.VerificationError):
            VERIFY.delivery_result(outcomes, 0)

    def test_pause_annotation_uses_go_template_index(self):
        with mock.patch.object(VERIFY, "kubectl", return_value="1\n") as kubectl:
            value = VERIFY.current_pause_annotation()

        self.assertEqual(value, "1")
        self.assertIn("index .metadata.annotations", kubectl.call_args.args[-1])

        with mock.patch.object(VERIFY, "kubectl", return_value="<no value>\n"):
            self.assertIsNone(VERIFY.current_pause_annotation())

    def test_pod_watch_always_emits_all_five_fields(self):
        self.assertIn("{{else}}<no value>|<no value>|", VERIFY.PodWatch.template)

    def test_sink_baseline_accepts_null_or_empty_latched_slice(self):
        baseline = {
            "latency_ms": 0,
            "fail_rate": 0,
            "latched": None,
            "held": {},
        }
        with mock.patch.object(VERIFY, "sink_control_state", return_value=baseline):
            self.assertTrue(VERIFY.sink_is_at_baseline("http://sink"))

        baseline["latched"] = []
        with mock.patch.object(VERIFY, "sink_control_state", return_value=baseline):
            self.assertTrue(VERIFY.sink_is_at_baseline("http://sink"))

    def test_running_image_revision_must_match_source_commit(self):
        source_commit = "a" * 40
        image_id = "docker://sha256:" + "b" * 64
        pod = {"status": {"containerStatuses": [{"imageID": image_id}]}}
        inspected = json.dumps(
            {
                "info": {
                    "imageSpec": {
                        "config": {
                            "Labels": {"org.opencontainers.image.revision": "a" * 7}
                        }
                    }
                }
            }
        )
        with mock.patch.object(VERIFY, "command", return_value=inspected) as command:
            provenance = VERIFY.image_provenance(pod, source_commit)

        self.assertEqual(provenance["image_id"], image_id)
        self.assertEqual(provenance["image_revision"], "a" * 7)
        self.assertEqual(command.call_args.args[0][-1], "sha256:" + "b" * 64)

        mismatch = inspected.replace("a" * 7, "c" * 7)
        with (
            mock.patch.object(VERIFY, "command", return_value=mismatch),
            self.assertRaisesRegex(VERIFY.VerificationError, "does not match"),
        ):
            VERIFY.image_provenance(pod, source_commit)

    def test_output_must_be_new_and_beneath_private_evidence_root(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            local_evidence = root / ".evidence" / "m4-local"
            with (
                mock.patch.object(VERIFY, "ROOT", root),
                mock.patch.object(VERIFY, "LOCAL_EVIDENCE_ROOT", local_evidence),
            ):
                accepted = local_evidence / "run" / "sigterm.json"
                VERIFY.validate_output(accepted)
                VERIFY.validate_output(
                    Path(".evidence/m4-local/relative-run/sigterm.json")
                )

                with self.assertRaisesRegex(VERIFY.VerificationError, "beneath"):
                    VERIFY.validate_output(root / "docs" / "sigterm.json")

                accepted.parent.mkdir(parents=True)
                accepted.write_text("{}\n", encoding="utf-8")
                with self.assertRaisesRegex(VERIFY.VerificationError, "already exists"):
                    VERIFY.validate_output(accepted)


if __name__ == "__main__":
    unittest.main()
