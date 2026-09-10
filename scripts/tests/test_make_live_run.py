from __future__ import annotations

import os
from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]


class LiveRunMakeTargetTest(unittest.TestCase):
    def test_runtime_bootstrap_target_reaches_the_nested_go_module(self):
        with tempfile.TemporaryDirectory() as go_cache:
            environment = os.environ.copy()
            environment["GOCACHE"] = go_cache
            result = subprocess.run(
                [
                    "make",
                    "--no-print-directory",
                    "aws-runtime-bootstrap",
                    "AWS_RUN_ID=invalid",
                    f"AWS_APPROVED_COMMIT={'a' * 40}",
                    "AWS_REAL_ENV=env",
                ],
                cwd=ROOT,
                env=environment,
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )

        combined = result.stdout + result.stderr
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("AWS_RUN_ID must use UTC YYYYMMDDTHHMMSSZ", combined)
        self.assertNotIn("cannot find main module", combined)

    def test_status_target_reaches_the_nested_go_module(self):
        with tempfile.TemporaryDirectory() as go_cache:
            environment = os.environ.copy()
            environment["GOCACHE"] = go_cache
            result = subprocess.run(
                [
                    "make",
                    "--no-print-directory",
                    "aws-live-status",
                    "AWS_RUN_ID=invalid",
                ],
                cwd=ROOT,
                env=environment,
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )

        combined = result.stdout + result.stderr
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("AWS_RUN_ID must use UTC YYYYMMDDTHHMMSSZ", combined)
        self.assertNotIn("cannot find main module", combined)

    def test_guardrail_plan_removes_an_older_plan_before_planning(self):
        with tempfile.TemporaryDirectory() as temporary:
            plan = Path(temporary) / "guardrails.tfplan"
            plan.write_text("stale", encoding="utf-8")
            result = subprocess.run(
                [
                    "make",
                    "--no-print-directory",
                    "aws-guardrails-plan",
                    f"AWS_GUARDRAILS_PLAN_FILE={plan}",
                    "AWS_REAL_ENV=true",
                    "MAKE=true",
                ],
                cwd=ROOT,
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertFalse(plan.exists())

    def test_guardrail_apply_consumes_the_reviewed_plan(self):
        with tempfile.TemporaryDirectory() as temporary:
            plan = Path(temporary) / "guardrails.tfplan"
            plan.write_text("reviewed", encoding="utf-8")
            result = subprocess.run(
                [
                    "make",
                    "--no-print-directory",
                    "aws-guardrails-up",
                    f"AWS_GUARDRAILS_PLAN_FILE={plan}",
                    "AWS_REAL_ENV=true",
                ],
                cwd=ROOT,
                input="yes\n",
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertFalse(plan.exists())

    def test_environment_controller_pid_does_not_skip_aws_up_confirmation(self):
        environment = os.environ.copy()
        environment["MLP_AWS_LIVE_CONTROLLER_PID"] = "1"
        result = subprocess.run(
            [
                "make",
                "--no-print-directory",
                "aws-up",
                "AWS_RUN_ID=20260908T000000Z",
                f"AWS_APPROVED_COMMIT={'a' * 40}",
                "AWS_REAL_ENV=true",
                "MAKE=true",
            ],
            cwd=ROOT,
            env=environment,
            stdin=subprocess.DEVNULL,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        self.assertNotEqual(result.returncode, 0)


if __name__ == "__main__":
    unittest.main()
