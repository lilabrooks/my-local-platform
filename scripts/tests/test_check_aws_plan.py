from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).parents[1] / "check-aws-plan.py"
SPEC = importlib.util.spec_from_file_location("check_aws_plan", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
CHECK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECK)
ROOT = SCRIPT.parent.parent
GUARD = SCRIPT.parent / "aws-terraform-guard.sh"


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
        counts = {resource_type: 0 for resource_type in CHECK.HOURLY_RESOURCE_LIMITS}
        self.assertEqual(CHECK.gate_failures(shape(), counts, counts, []), [])

    def test_disabled_resource_creation_fails(self):
        planned = {resource_type: 0 for resource_type in CHECK.HOURLY_RESOURCE_LIMITS}
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
        counts = {resource_type: 0 for resource_type in CHECK.HOURLY_RESOURCE_LIMITS}
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
        self.temp = Path(self.temporary.name)
        self.bin = self.temp / "bin"
        self.bin.mkdir()
        self.plan = self.temp / "reviewed.tfplan"
        self.summary = self.temp / "summary.json"
        self.log = self.temp / "calls.log"
        account_id = "".join(("1234", "5678", "9012"))

        runtime = {
            "hourly_enabled": True,
            "enable_eks": True,
            "budget_name": "mlp-dev-live-runtime",
            "region": "us-east-1",
            "eks_version": "1.35",
        }
        missing_runtime_keys = {"budget_name": "mlp-dev-live-runtime"}
        planned_shape = shape(
            hourly_enabled=True,
            enable_eks=True,
            expected_hourly_usd=0.2332,
        )
        plan_json = {
            "complete": True,
            "planned_values": {
                "outputs": {
                    "runtime_budget_name": {"value": "mlp-dev-live-runtime"},
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

        terraform = f"""#!/bin/sh
printf 'terraform %s\\n' "$*" >> "$MLP_FAKE_LOG"
case " $* " in
  *' console '*)
    if [ "${{MLP_FAKE_RUNTIME_MISSING_KEYS:-}}" = 1 ]; then
      printf '%s\\n' '{json.dumps(json.dumps(missing_runtime_keys))}'
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
    printf '%s\\n' '[{"NotificationType":"ACTUAL","ComparisonOperator":"GREATER_THAN","Threshold":80,"ThresholdType":"PERCENTAGE"}]'
    ;;
  'budgets describe-subscribers-for-notification')
    printf '%s\\n' "${MLP_FAKE_SUBSCRIBER_COUNT:-1}"
    ;;
  'eks describe-cluster-versions') printf '1\\n' ;;
  *) exit 2 ;;
esac
""".replace("@ACCOUNT@", account_id)
        self._write_executable("terraform", terraform)
        self._write_executable("aws", aws)

        self.environment = os.environ.copy()
        self.environment.update(
            {
                "PATH": f"{self.bin}:{self.environment['PATH']}",
                "MLP_AWS_PLAN_FILE": str(self.plan),
                "MLP_AWS_PLAN_SUMMARY": str(self.summary),
                "MLP_FAKE_LOG": str(self.log),
            }
        )

    def tearDown(self):
        self.temporary.cleanup()

    def _write_executable(self, name: str, body: str):
        path = self.bin / name
        path.write_text(body, encoding="utf-8")
        path.chmod(0o755)

    def _run(self, action: str, environment=None, shell=None):
        command = [str(GUARD), action]
        if shell is not None:
            command = [shell, str(GUARD), action]
        return subprocess.run(
            command,
            cwd=ROOT,
            env=environment or self.environment,
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    def test_support_check_is_immediately_before_plan_and_apply(self):
        planned = self._run("plan")
        self.assertEqual(planned.returncode, 0, planned.stderr)
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

    def test_missing_budget_blocks_plan(self):
        environment = self.environment.copy()
        environment["MLP_FAKE_BUDGET_MISSING"] = "1"

        result = self._run("plan", environment)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("pre-existing mlp-dev-live-runtime budget", result.stderr)
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

    def test_missing_budget_subscriber_blocks_plan(self):
        environment = self.environment.copy()
        environment["MLP_FAKE_SUBSCRIBER_COUNT"] = "0"

        result = self._run("plan", environment)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("no notification subscriber", result.stderr)
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

    def test_changed_plan_is_not_applied(self):
        planned = self._run("plan")
        self.assertEqual(planned.returncode, 0, planned.stderr)
        self.plan.write_text("changed-after-review", encoding="utf-8")

        applied = self._run("apply")

        self.assertNotEqual(applied.returncode, 0)
        self.assertIn("changed after review", applied.stderr)
        calls = self.log.read_text(encoding="utf-8").splitlines()
        self.assertFalse(any(" apply " in f" {call} " for call in calls))


if __name__ == "__main__":
    unittest.main()
