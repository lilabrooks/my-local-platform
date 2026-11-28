from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]


class LiveRunMakeTargetTest(unittest.TestCase):
    def test_db_mcp_pilot_init_selects_and_checks_its_own_state(self):
        stack = Path("infra/terraform/envs/db-mcp-pilot")
        key = "envs/db-mcp-pilot/terraform.tfstate"
        self.assertRegex(
            (ROOT / stack / "main.tf").read_text(),
            r'backend\s+"s3"\s*\{[^}]*?\bkey\s*=\s*"envs/db-mcp-pilot/terraform\.tfstate"',
        )
        account = "0" * 12
        bucket = f"mlp-tfstate-{account}"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            cache = root / stack / ".terraform"
            cache.mkdir(parents=True)
            aws = root / "aws"
            aws.write_text(f"#!/bin/sh\necho {account}\n")
            terraform = root / "terraform"
            terraform.write_text(
                "#!/bin/sh\n"
                f'[ "$1" = "-chdir={stack}" ] || exit 2\n'
                'shift\n'
                f'if [ "$*" = "init -input=false -reconfigure -backend-config=bucket={bucket}" ]; then exit 0; fi\n'
                '[ "$*" = "workspace show" ] && cat workspace\n'
            )
            for executable in (aws, terraform):
                executable.chmod(0o700)
            for state_key, workspace, success in (
                (key, "default", True),
                ("envs/dev/terraform.tfstate", "default", False),
                (key, "other", False),
            ):
                with self.subTest(key=state_key, workspace=workspace):
                    (root / "workspace").write_text(workspace)
                    (cache / "terraform.tfstate").write_text(json.dumps({
                        "backend": {"type": "s3", "config": {"key": state_key, "bucket": bucket}}
                    }))
                    result = subprocess.run(
                        ["make", "-f", str(ROOT / "Makefile"), "db-mcp-pilot-init",
                         "AWS_INIT_ARGS=-backend-config=key=envs/dev/terraform.tfstate"],
                        cwd=root, env={**os.environ, "PATH": f"{root}:{os.environ['PATH']}"},
                        capture_output=True, text=True, check=False,
                    )
                    self.assertEqual(result.returncode == 0, success, result.stderr)

    def test_runtime_bootstrap_target_reaches_the_nested_go_module_without_running(self):
        result = subprocess.run(
            [
                "make",
                "--no-print-directory",
                "--dry-run",
                "aws-runtime-bootstrap",
                "AWS_RUN_ID=20260909T200000Z",
                f"AWS_APPROVED_COMMIT={'a' * 40}",
            ],
            cwd=ROOT,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        combined = result.stdout + result.stderr
        self.assertEqual(result.returncode, 0, combined)
        self.assertIn("go -C tools/m4-bootstrap run .", combined)
        self.assertNotIn("ResourceNotFoundException", combined)

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

    def test_live_stop_records_the_requested_reason(self):
        for extra, reason in (
            ([], "evidence_complete"),
            (["AWS_STOP_REASON=infrastructure_failed"], "infrastructure_failed"),
        ):
            with self.subTest(reason=reason):
                result = subprocess.run(
                    [
                        "make",
                        "--no-print-directory",
                        "--dry-run",
                        "aws-live-stop",
                        "AWS_RUN_ID=20260908T000000Z",
                        *extra,
                    ],
                    cwd=ROOT,
                    check=False,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )

                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn(f'--reason "{reason}"', result.stdout)


if __name__ == "__main__":
    unittest.main()
