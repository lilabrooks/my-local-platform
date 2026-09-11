from __future__ import annotations

import importlib.util
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
import shutil
from unittest import mock


SCRIPT = Path(__file__).parents[1] / "check-aws-plan.py"
SPEC = importlib.util.spec_from_file_location("check_aws_plan", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
CHECK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECK)
ROOT = SCRIPT.parent.parent


def shape(**overrides):
    value = {
        "region": "us-east-1",
        "hourly_enabled": False,
        "enable_eks": False,
        "enable_msk": False,
        "enable_rds": False,
        "expected_hourly_usd": 0,
        "maximum_hourly_usd": 1.25,
        "eks": {
            "kubernetes_version": "1.35",
            "node_capacity_type": "SPOT",
            "node_desired": 2,
            "node_maximum": 3,
        },
        "kafka": {
            "delivery_partitions": 12,
            "dead_letter_partitions": 1,
            "total_partitions": 13,
        },
    }
    value.update(overrides)
    return value


class PlanShapeTest(unittest.TestCase):
    def test_summary_destination_is_confined_and_rejects_symlinks(self):
        run_id = "20260908T050000Z"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "repository"
            run = root / ".evidence" / "m4" / run_id
            run.mkdir(parents=True)
            summary = run / "03-plan-summary.json"
            with mock.patch.object(CHECK, "ROOT", root):
                self.assertEqual(
                    CHECK.validate_summary_destination(run_id, summary, False),
                    summary.absolute(),
                )
                with self.assertRaisesRegex(SystemExit, "directly under"):
                    CHECK.validate_summary_destination(
                        run_id, root / "summary.json", False
                    )
                target = Path(temporary) / "outside.json"
                target.write_text("{}", encoding="utf-8")
                summary.symlink_to(target)
                with self.assertRaisesRegex(SystemExit, "must not be a symlink"):
                    CHECK.validate_summary_destination(run_id, summary, False)

    def test_counts_nested_hourly_resources_without_values(self):
        plan = {
            "planned_values": {
                "root_module": {
                    "resources": [{"type": "aws_db_instance"}],
                    "child_modules": [
                        {
                            "resources": [
                                {"type": "aws_eks_cluster"},
                                {"type": "aws_eks_node_group"},
                                {"type": "aws_nat_gateway"},
                            ]
                        }
                    ],
                }
            },
            "resource_changes": [
                {
                    "type": "aws_msk_serverless_cluster",
                    "change": {"actions": ["create"]},
                },
                {
                    "type": "aws_db_instance",
                    "change": {"actions": ["no-op"]},
                },
            ],
        }

        planned = CHECK.planned_resource_type_counts(plan)
        creates = CHECK.create_resource_type_counts(plan)
        self.assertEqual(planned["aws_db_instance"], 1)
        self.assertEqual(planned["aws_eks_cluster"], 1)
        self.assertEqual(creates["aws_msk_serverless_cluster"], 1)
        self.assertNotIn("aws_db_instance", creates)

    def test_default_shape_passes_with_no_hourly_resources(self):
        counts = {resource_type: 0 for resource_type in CHECK.HOURLY_RESOURCE_TYPES}
        self.assertEqual(CHECK.gate_failures(shape(), counts, counts, []), [])

    def test_disabled_resource_creation_fails(self):
        planned = {resource_type: 0 for resource_type in CHECK.HOURLY_RESOURCE_TYPES}
        creates = planned.copy()
        planned["aws_msk_serverless_cluster"] = 1
        creates["aws_msk_serverless_cluster"] = 1

        failures = CHECK.gate_failures(shape(), planned, creates, [])

        self.assertIn(
            "aws_msk_serverless_cluster is planned while its flag is disabled",
            failures,
        )
        self.assertIn(
            "aws_msk_serverless_cluster is created while its flag is disabled",
            failures,
        )

    def test_cost_partition_and_node_limits_fail_closed(self):
        counts = {resource_type: 0 for resource_type in CHECK.HOURLY_RESOURCE_TYPES}
        runtime = shape(
            enable_eks=True,
            expected_hourly_usd=1.26,
            eks={
                "kubernetes_version": "1.35",
                "node_capacity_type": "ON_DEMAND",
                "node_desired": 4,
                "node_maximum": 4,
            },
            kafka={
                "delivery_partitions": 13,
                "dead_letter_partitions": 1,
                "total_partitions": 14,
            },
        )

        failures = CHECK.gate_failures(runtime, counts, counts, [])

        self.assertTrue(any("hourly cost" in failure for failure in failures))
        self.assertTrue(any("Kafka partition count" in failure for failure in failures))
        self.assertIn(
            "aws_eks_cluster planned count 0 must be 1 when enabled", failures
        )
        self.assertIn("EKS node capacity type is not SPOT", failures)
        self.assertTrue(any("EKS node maximum" in failure for failure in failures))

    def test_replacement_of_enabled_hourly_resources_fails(self):
        plan = {
            "planned_values": {
                "root_module": {
                    "resources": [
                        {"type": "aws_eks_cluster"},
                        {"type": "aws_eks_node_group"},
                        {"type": "aws_msk_serverless_cluster"},
                        {"type": "aws_nat_gateway"},
                    ]
                }
            },
            "resource_changes": [
                {
                    "address": "module.eks[0].aws_eks_cluster.this[0]",
                    "type": "aws_eks_cluster",
                    "change": {"actions": ["delete", "create"]},
                },
                {
                    "address": "aws_msk_serverless_cluster.relay[0]",
                    "type": "aws_msk_serverless_cluster",
                    "change": {"actions": ["create", "delete"]},
                },
            ],
        }
        planned = CHECK.hourly_counts(CHECK.planned_resource_type_counts(plan))
        creates = CHECK.hourly_counts(CHECK.create_resource_type_counts(plan))
        changes = CHECK.hourly_changes(plan)

        failures = CHECK.gate_failures(
            shape(hourly_enabled=True, enable_eks=True, enable_msk=True),
            planned,
            creates,
            changes,
        )

        self.assertIn(
            "module.eks[0].aws_eks_cluster.this[0] would delete an enabled hourly resource",
            failures,
        )
        self.assertIn(
            "aws_msk_serverless_cluster.relay[0] would delete an enabled hourly resource",
            failures,
        )


class GuardScriptTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.temp = Path(self.temporary.name).resolve()
        self.repository = self.temp / "repository"
        scripts = self.repository / "scripts"
        terraform_directory = self.repository / "infra" / "terraform" / "envs" / "dev"
        scripts.mkdir(parents=True)
        (terraform_directory / ".terraform").mkdir(parents=True)
        for script_name in (
            "aws-terraform-guard.sh",
            "check-aws-plan.py",
            "m4-evidence.py",
            "m4-stage.py",
        ):
            shutil.copy2(ROOT / "scripts" / script_name, scripts / script_name)
        self.guard = scripts / "aws-terraform-guard.sh"
        self.bin = self.temp / "bin"
        self.bin.mkdir()
        self.plan = terraform_directory / ".terraform" / "mlp-reviewed.tfplan"
        self.log = self.temp / "calls.log"
        account_id = "".join(("1234", "5678", "9012"))
        source_commit = "a" * 40
        evidence_root = self.repository / ".evidence" / "m4"
        run_id = datetime(1970, 1, 1, tzinfo=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        evidence = evidence_root / run_id
        evidence.mkdir(parents=True)
        self.run_id = run_id
        self.evidence = evidence
        self.summary = evidence / "03-plan-summary.json"
        (evidence / "00-preflight.json").write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "run_id": run_id,
                    "commit": source_commit,
                    "result": "passed",
                }
            ),
            encoding="utf-8",
        )

        runtime = {
            "hourly_enabled": True,
            "enable_eks": True,
            "budget_name": "mlp-live-aws-monthly",
            "region": "us-east-1",
            "eks_version": "1.35",
        }
        cheap_runtime = {
            **runtime,
            "hourly_enabled": False,
            "enable_eks": False,
        }
        missing_runtime_keys = {"budget_name": "mlp-live-aws-monthly"}
        planned_shape = shape(
            hourly_enabled=True,
            enable_eks=True,
            expected_hourly_usd=0.2332,
        )
        plan_json = {
            "complete": True,
            "planned_values": {
                "outputs": {
                    "runtime_budget_name": {"value": "mlp-live-aws-monthly"},
                    "runtime_shape": {"value": planned_shape},
                },
                "root_module": {
                    "resources": [{"type": "aws_eks_cluster"}],
                    "child_modules": [
                        {
                            "resources": [
                                {"type": "aws_eks_node_group"},
                                {"type": "aws_nat_gateway"},
                            ]
                        }
                    ],
                },
            },
            "resource_changes": [],
        }
        incomplete_plan_json = {**plan_json, "complete": False}
        cheap_plan_json = {
            **plan_json,
            "planned_values": {
                "outputs": {
                    "runtime_budget_name": {"value": "mlp-live-aws-monthly"},
                    "runtime_shape": {"value": shape()},
                },
                "root_module": {"resources": []},
            },
        }

        terraform = f"""#!/bin/sh
printf 'terraform %s\\n' "$*" >> "$MLP_FAKE_LOG"
case " $* " in
  *' console '*)
    if [ "${{MLP_FAKE_RUNTIME_MISSING_KEYS:-}}" = 1 ]; then
      printf '%s\\n' '{json.dumps(json.dumps(missing_runtime_keys))}'
    elif [ "${{MLP_FAKE_CHEAP:-}}" = 1 ]; then
      printf '%s\\n' '{json.dumps(json.dumps(cheap_runtime))}'
    else
      printf '%s\\n' '{json.dumps(json.dumps(runtime))}'
    fi
    ;;
  *' plan '*)
    [ "${{MLP_FAKE_PLAN_FAIL:-}}" != 1 ] || exit 43
    for argument in "$@"; do
      case "$argument" in -out=*) plan_file=${{argument#-out=}} ;; esac
    done
    printf 'reviewed-plan' > "$plan_file"
    ;;
  *' show '*)
    if [ "${{MLP_FAKE_INCOMPLETE_PLAN:-}}" = 1 ]; then
      printf '%s\\n' '{json.dumps(incomplete_plan_json)}'
    elif [ "${{MLP_FAKE_CHEAP:-}}" = 1 ]; then
      printf '%s\\n' '{json.dumps(cheap_plan_json)}'
    else
      printf '%s\\n' '{json.dumps(plan_json)}'
    fi
    ;;
  *' apply '*) exit 0 ;;
  *) exit 2 ;;
esac
"""
        aws = """#!/bin/sh
printf 'aws %s\\n' "$*" >> "$MLP_FAKE_LOG"
case "$1 $2" in
  'sts get-caller-identity') printf '@ACCOUNT@\\n' ;;
  'budgets describe-budget')
    [ "${MLP_FAKE_BUDGET_MISSING:-}" != 1 ] || exit 42
    printf '%s\\n' "${MLP_FAKE_BUDGET_LIMIT:-5.0}"
    ;;
  'budgets describe-notifications-for-budget')
    if [ "${MLP_FAKE_BUDGET_ALARM:-}" = 1 ]; then alarm=',"NotificationState":"ALARM"'; else alarm=',"NotificationState":"OK"'; fi
    printf '%s\\n' "[{\\"NotificationType\\":\\"ACTUAL\\",\\"ComparisonOperator\\":\\"GREATER_THAN\\",\\"Threshold\\":80.0,\\"ThresholdType\\":\\"PERCENTAGE\\"$alarm},{\\"NotificationType\\":\\"ACTUAL\\",\\"ComparisonOperator\\":\\"GREATER_THAN\\",\\"Threshold\\":100.0,\\"ThresholdType\\":\\"PERCENTAGE\\",\\"NotificationState\\":\\"OK\\"},{\\"NotificationType\\":\\"FORECASTED\\",\\"ComparisonOperator\\":\\"GREATER_THAN\\",\\"Threshold\\":100.0,\\"ThresholdType\\":\\"PERCENTAGE\\",\\"NotificationState\\":\\"OK\\"}]"
    ;;
  'budgets describe-subscribers-for-notification')
    printf '%s\\n' "${MLP_FAKE_SUBSCRIBER_COUNT:-1}"
    ;;
  'eks describe-cluster-versions') printf '1\\n' ;;
  *) exit 2 ;;
esac
""".replace("@ACCOUNT@", account_id)
        git = f"""#!/bin/sh
printf 'git %s\\n' "$*" >> "$MLP_FAKE_LOG"
case " $* " in
  *' rev-parse HEAD ') printf '%s\\n' "${{MLP_FAKE_HEAD:-{source_commit}}}" ;;
  *' status --porcelain --untracked-files=all ') printf '%s' "${{MLP_FAKE_DIRTY:-}}" ;;
  *) exit 2 ;;
esac
"""
        self._write_executable("terraform", terraform)
        self._write_executable("aws", aws)
        self._write_executable("git", git)

        self.environment = os.environ.copy()
        self.environment.update(
            {
                "PATH": f"{self.bin}:{self.environment['PATH']}",
                "MLP_AWS_PLAN_FILE": str(self.plan),
                "MLP_AWS_PLAN_SUMMARY": str(self.summary),
                "MLP_AWS_APPROVED_COMMIT": source_commit,
                "AWS_RUN_ID": run_id,
                "MLP_FAKE_LOG": str(self.log),
                "MLP_AWS_LIVE_CONTROLLER_PID": str(os.getpid()),
            }
        )

    def tearDown(self):
        self.temporary.cleanup()

    def _write_executable(self, name: str, body: str):
        path = self.bin / name
        path.write_text(body, encoding="utf-8")
        path.chmod(0o755)

    def _run(self, action: str, environment=None, shell=None, arguments=None):
        command = [str(self.guard), action, *(arguments or [])]
        if shell is not None:
            command = [shell, str(self.guard), action, *(arguments or [])]
        return subprocess.run(
            command,
            cwd=self.repository,
            env=environment or self.environment,
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    def _write_go_packet(self):
        from scripts.tests.test_m4_stage import ImageStageTest

        fixture = self.evidence.parent.parent.parent / "release-fixture"
        release = ImageStageTest().write_release_inputs(fixture, self.run_id)
        names = {
            "preflight": "00-preflight.json",
            "identity": "01-identity.txt",
            "prices_markdown": "02-prices.md",
            "prices": "02-prices.json",
            "plan": "03-plan-summary.json",
            "inventory": "04-inventory-before.json",
            "images": "05-images.json",
            "capture_plan": "capture-plan.json",
        }
        for key, filename in names.items():
            if key not in {"preflight", "plan"}:
                (self.evidence / filename).write_bytes(
                    (release / filename).read_bytes()
                )
        packet = {
            "schema_version": 1,
            "run_id": self.run_id,
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "decision": "go",
            "source_commit": "a" * 40,
            "region": "us-east-1",
            "plan": {
                "sha256": hashlib.sha256(self.plan.read_bytes()).hexdigest(),
            },
            "cleanup_owner": "test-operator",
            "abort_command": "make aws-down",
            "input_sha256": {
                key: hashlib.sha256((self.evidence / name).read_bytes()).hexdigest()
                for key, name in names.items()
            },
            "gate": {"passed": True, "failures": []},
        }
        (self.evidence / "06-go-no-go.json").write_text(
            json.dumps(packet), encoding="utf-8"
        )
        started = datetime.now(timezone.utc).replace(microsecond=0)
        (self.evidence / "00-session.json").write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "run_id": self.run_id,
                    "commit": "a" * 40,
                    "region": "us-east-1",
                    "operator": "test-operator",
                    "billable_started_at": started.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "destroy_deadline": (started + timedelta(minutes=150)).strftime(
                        "%Y-%m-%dT%H:%M:%SZ"
                    ),
                    "hard_deadline": (started + timedelta(minutes=180)).strftime(
                        "%Y-%m-%dT%H:%M:%SZ"
                    ),
                    "limits": {
                        "maximum_hourly_usd": 1.25,
                        "maximum_total_usd": 5.0,
                    },
                }
            ),
            encoding="utf-8",
        )
        (self.evidence / "controller-state.json").write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "run_id": self.run_id,
                    "commit": "a" * 40,
                    "region": "us-east-1",
                    "controller_pid": os.getpid(),
                    "phase": "applying",
                    "updated_at": started.strftime("%Y-%m-%dT%H:%M:%SZ"),
                }
            ),
            encoding="utf-8",
        )

    def test_support_check_is_immediately_before_plan_and_apply(self):
        planned = self._run("plan")
        self.assertEqual(planned.returncode, 0, planned.stderr)
        self._write_go_packet()
        calls = self.log.read_text(encoding="utf-8").splitlines()
        support = next(i for i, call in enumerate(calls) if call.startswith("aws eks "))
        plan = next(i for i, call in enumerate(calls) if " plan " in f" {call} ")
        self.assertEqual(support + 1, plan)

        self.log.write_text("", encoding="utf-8")
        applied = self._run("apply")
        self.assertEqual(applied.returncode, 0, applied.stderr)
        calls = self.log.read_text(encoding="utf-8").splitlines()
        support = next(i for i, call in enumerate(calls) if call.startswith("aws eks "))
        apply = next(i for i, call in enumerate(calls) if " apply " in f" {call} ")
        self.assertEqual(support + 1, apply)

    def test_plan_summary_is_bound_to_run_and_commit(self):
        result = self._run("plan")
        self.assertEqual(result.returncode, 0, result.stderr)
        summary = json.loads(self.summary.read_text(encoding="utf-8"))
        self.assertEqual(summary["run_id"], self.run_id)
        self.assertEqual(summary["source_commit"], "a" * 40)
        self.assertRegex(summary["captured_at"], r"^\d{4}-\d{2}-\d{2}T")
        self.assertRegex(summary["terraform_input_sha256"], r"^[0-9a-f]{64}$")

    def test_terraform_argument_changes_the_input_digest(self):
        first = self._run("plan")
        self.assertEqual(first.returncode, 0, first.stderr)
        original = json.loads(self.summary.read_text(encoding="utf-8"))[
            "terraform_input_sha256"
        ]

        second = self._run("plan", arguments=["--var=enable_eks=true"])
        self.assertEqual(second.returncode, 0, second.stderr)
        changed = json.loads(self.summary.read_text(encoding="utf-8"))[
            "terraform_input_sha256"
        ]

        self.assertNotEqual(original, changed)

    def test_source_mismatch_or_dirty_worktree_blocks_plan(self):
        wrong = self.environment.copy()
        wrong["MLP_FAKE_HEAD"] = "b" * 40
        mismatch = self._run("plan", wrong)
        self.assertNotEqual(mismatch.returncode, 0)
        self.assertIn("does not match approved commit", mismatch.stderr)

        dirty = self.environment.copy()
        dirty["MLP_FAKE_DIRTY"] = " M infra/terraform/envs/dev/main.tf"
        changed = self._run("plan", dirty)
        self.assertNotEqual(changed.returncode, 0)
        self.assertIn("worktree must be clean", changed.stderr)

    def test_missing_budget_blocks_plan(self):
        environment = self.environment.copy()
        environment["MLP_FAKE_BUDGET_MISSING"] = "1"

        result = self._run("plan", environment)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("pre-existing mlp-live-aws-monthly budget", result.stderr)
        self.assertFalse(self.plan.exists())

    def test_plan_with_no_variable_arguments_runs_under_system_bash(self):
        result = self._run("plan", shell="/bin/bash")

        self.assertEqual(result.returncode, 0, result.stderr)

    def test_missing_runtime_shape_boolean_blocks_plan(self):
        environment = self.environment.copy()
        environment["MLP_FAKE_RUNTIME_MISSING_KEYS"] = "1"

        result = self._run("plan", environment)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("invalid hourly_enabled", result.stderr)
        self.assertFalse(self.plan.exists())

    def test_incomplete_plan_is_rejected(self):
        environment = self.environment.copy()
        environment["MLP_FAKE_INCOMPLETE_PLAN"] = "1"

        result = self._run("plan", environment)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("targeted and destroy plans are rejected", result.stderr)
        self.assertFalse(self.summary.exists())

    def test_invalid_budget_limit_blocks_plan(self):
        environment = self.environment.copy()
        environment["MLP_FAKE_BUDGET_LIMIT"] = "unknown"

        result = self._run("plan", environment)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("invalid budget limit", result.stderr)
        self.assertFalse(self.plan.exists())

    def test_changed_budget_limit_blocks_plan(self):
        environment = self.environment.copy()
        environment["MLP_FAKE_BUDGET_LIMIT"] = "4"

        result = self._run("plan", environment)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("approved $5 limit", result.stderr)
        self.assertFalse(self.plan.exists())

    def test_missing_budget_subscriber_blocks_plan(self):
        environment = self.environment.copy()
        environment["MLP_FAKE_SUBSCRIBER_COUNT"] = "0"

        result = self._run("plan", environment)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("notification without a subscriber", result.stderr)
        self.assertFalse(self.plan.exists())

    def test_active_budget_alarm_blocks_plan(self):
        environment = self.environment.copy()
        environment["MLP_FAKE_BUDGET_ALARM"] = "1"

        result = self._run("plan", environment)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("not all OK", result.stderr)
        self.assertFalse(self.plan.exists())

    def test_failed_replan_removes_the_previous_reviewed_pair(self):
        first = self._run("plan")
        self.assertEqual(first.returncode, 0, first.stderr)
        self.assertTrue(self.plan.exists())
        self.assertTrue(self.summary.exists())
        environment = self.environment.copy()
        environment["MLP_FAKE_PLAN_FAIL"] = "1"

        second = self._run("plan", environment)

        self.assertNotEqual(second.returncode, 0)
        self.assertFalse(self.plan.exists())
        self.assertFalse(self.summary.exists())

    def test_plan_rejects_external_artifact_paths_before_deleting_them(self):
        for variable in (
            "MLP_AWS_PLAN_FILE",
            "MLP_AWS_PLAN_SUMMARY",
            "MLP_AWS_GO_NO_GO",
        ):
            with self.subTest(variable=variable):
                sentinel = self.temp / f"{variable}.sentinel"
                sentinel.write_text("keep me", encoding="utf-8")
                environment = self.environment.copy()
                environment[variable] = str(sentinel)

                result = self._run("plan", environment)

                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(sentinel.read_text(encoding="utf-8"), "keep me")

    def test_changed_plan_is_not_applied(self):
        planned = self._run("plan")
        self.assertEqual(planned.returncode, 0, planned.stderr)
        self._write_go_packet()
        self.plan.write_text("changed-after-review", encoding="utf-8")

        applied = self._run("apply")

        self.assertNotEqual(applied.returncode, 0)
        self.assertIn("changed after review", applied.stderr)
        calls = self.log.read_text(encoding="utf-8").splitlines()
        self.assertFalse(any(" apply " in f" {call} " for call in calls))

    def test_missing_go_packet_blocks_apply(self):
        planned = self._run("plan")
        self.assertEqual(planned.returncode, 0, planned.stderr)

        applied = self._run("apply")

        self.assertNotEqual(applied.returncode, 0)
        self.assertIn("GO packet not found", applied.stderr)

    def test_hourly_apply_requires_live_controller(self):
        planned = self._run("plan")
        self.assertEqual(planned.returncode, 0, planned.stderr)
        self._write_go_packet()
        environment = self.environment.copy()
        environment.pop("MLP_AWS_LIVE_CONTROLLER_PID")

        applied = self._run("apply", environment)

        self.assertNotEqual(applied.returncode, 0)
        self.assertIn("must run under make aws-live-run", applied.stderr)

    def test_cheap_tier_apply_does_not_require_go_packet(self):
        environment = self.environment.copy()
        environment["MLP_FAKE_CHEAP"] = "1"
        planned = self._run("plan", environment)
        self.assertEqual(planned.returncode, 0, planned.stderr)

        applied = self._run("apply", environment)

        self.assertEqual(applied.returncode, 0, applied.stderr)

    def test_terraform_cli_environment_is_rejected(self):
        environment = self.environment.copy()
        environment["TF_CLI_ARGS_plan"] = "-var=enable_eks=true"

        result = self._run("plan", environment)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("TF_CLI_ARGS_plan is not accepted", result.stderr)

    def test_double_dash_var_reaches_console_and_plan(self):
        result = self._run("plan", arguments=["--var=enable_eks=true"])

        self.assertEqual(result.returncode, 0, result.stderr)
        calls = self.log.read_text(encoding="utf-8").splitlines()
        terraform_calls = [call for call in calls if call.startswith("terraform ")]
        self.assertIn("--var=enable_eks=true", terraform_calls[0])
        self.assertIn("--var=enable_eks=true", terraform_calls[1])

    def test_double_dash_out_is_rejected(self):
        result = self._run("plan", arguments=["--out=elsewhere.tfplan"])

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("guarded plan owns -out", result.stderr)


if __name__ == "__main__":
    unittest.main()
