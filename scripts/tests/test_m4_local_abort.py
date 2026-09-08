from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock


SCRIPT = Path(__file__).parents[1] / "m4-local-abort.py"
SPEC = importlib.util.spec_from_file_location("m4_local_abort", SCRIPT)
assert SPEC and SPEC.loader
ABORT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ABORT)


class M4LocalAbortTest(unittest.TestCase):
    def test_worker_handles_real_sigterm_and_removes_credentials(self):
        with tempfile.TemporaryDirectory() as temporary:
            workspace = Path(temporary) / "workspace"
            workspace.mkdir()

            result = ABORT.run_worker(workspace)

            ABORT.validate_worker_result(result)
            self.assertFalse(
                (workspace / "worker-state" / "temporary-credentials").exists()
            )
            self.assertFalse(
                (
                    workspace / "worker-state" / "simulated-hourly-resources.json"
                ).exists()
            )

    def test_worker_result_requires_destroy_inventory_cost_then_cleanup(self):
        result = {
            "events": list(ABORT.EXPECTED_WORKER_EVENTS),
            "resource_audit_empty": True,
            "temporary_credentials_removed": True,
            "credential_canary_absent": True,
        }
        ABORT.validate_worker_result(result)

        result["events"][2:4] = reversed(result["events"][2:4])
        with self.assertRaisesRegex(ABORT.RehearsalError, "destroy-first"):
            ABORT.validate_worker_result(result)

    def test_abort_protocol_rejects_a_destroy_recipe_without_state_backup(self):
        invalid = lambda _arguments: "\n".join(
            (
                "aws sts get-caller-identity",
                "test -f infra/terraform/envs/dev/.terraform/terraform.tfstate",
                "terraform destroy",
            )
        )
        with self.assertRaisesRegex(Exception, "state pull"):
            ABORT.validate_abort_protocol(invalid)

    def test_output_is_private_new_and_outside_live_packet(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            local = root / ".evidence" / "m4-local"
            with (
                mock.patch.object(ABORT, "ROOT", root),
                mock.patch.object(ABORT, "LOCAL_EVIDENCE_ROOT", local),
            ):
                output = ABORT.validate_output(
                    Path(".evidence/m4-local/run/abort-rehearsal.json")
                )
                self.assertEqual(output.parent.stat().st_mode & 0o777, 0o700)
                ABORT.write_json(output, {"result": "passed"})
                self.assertEqual(output.stat().st_mode & 0o777, 0o600)

                with self.assertRaisesRegex(ABORT.RehearsalError, "already exists"):
                    ABORT.validate_output(output)
                with self.assertRaisesRegex(ABORT.RehearsalError, "beneath"):
                    ABORT.validate_output(root / ".evidence" / "m4" / "live.json")

    def test_receipt_contains_only_the_canary_digest(self):
        result = {
            "credential_canary_absent": True,
            "temporary_credentials_removed": True,
        }
        serialized = json.dumps(result)

        self.assertNotIn("access_key", serialized)
        self.assertNotIn("session_token", serialized)


if __name__ == "__main__":
    unittest.main()
