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
            "kubernetes_version": "1.36",
            "node_capacity_type": "SPOT",
            "node_desired": 3,
            "node_maximum": 3,
        },
        "rds": {"engine_version": "17.11"},
        "kafka": {
            "delivery_partitions": 12,
            "dead_letter_partitions": 1,
            "total_partitions": 13,
        },
    }
    value.update(overrides)
    return value


NODE_GROUP_MODULE = 'module.eks[0].module.eks_managed_node_group["default"]'
PINNED_ADDONS = {
    "vpc-cni": ("before_compute", "v1.22.4-eksbuild.3"),
    "eks-pod-identity-agent": ("before_compute", "v1.3.10-eksbuild.3"),
    "coredns": ("this", "v1.14.3-eksbuild.23"),
    "kube-proxy": ("this", "v1.36.0-eksbuild.25"),
}
OMIT = object()


def network_addons(changes=None):
    """Planned add-ons as the EKS module names them; None omits one."""
    selected = {**PINNED_ADDONS, **(changes or {})}
    resources = []
    for name, value in selected.items():
        if value is None:
            continue
        placement, version = value
        values = {"addon_name": name}
        if version is not None:
            values["addon_version"] = version
        resources.append({
            "address": f'module.eks[0].aws_eks_addon.{placement}["{name}"]',
            "mode": "managed",
            "type": "aws_eks_addon",
            "name": placement,
            "index": name,
            "values": values,
        })
    return resources


def cni_policy_attachment():
    return {
        "address": f'{NODE_GROUP_MODULE}.aws_iam_role_policy_attachment.this["AmazonEKS_CNI_Policy"]',
        "mode": "managed",
        "type": "aws_iam_role_policy_attachment",
        "name": "this",
        "index": "AmazonEKS_CNI_Policy",
        "values": {"policy_arn": "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy"},
    }


def eks_plan(addons=None, bootstrap=False, cni_policy=True):
    """The planned end state of the fixed EKS topology, shaped like the module."""
    cluster = {
        "address": "module.eks[0].aws_eks_cluster.this[0]",
        "mode": "managed",
        "type": "aws_eks_cluster",
        "name": "this",
        "index": 0,
        "values": {} if bootstrap is OMIT else {"bootstrap_self_managed_addons": bootstrap},
    }
    node_resources = [{
        "address": f"{NODE_GROUP_MODULE}.aws_eks_node_group.this[0]",
        "mode": "managed",
        "type": "aws_eks_node_group",
        "name": "this",
        "index": 0,
        "values": {
            "scaling_config": [{"desired_size": 3, "max_size": 3, "min_size": 1}],
            "capacity_type": "SPOT",
        },
    }]
    if cni_policy:
        node_resources.append(cni_policy_attachment())
    return {"planned_values": {"root_module": {"child_modules": [{
        "address": "module.eks[0]",
        "resources": [cluster, *network_addons(addons)],
        "child_modules": [{"address": NODE_GROUP_MODULE, "resources": node_resources}],
    }]}}}


class EksDependencyReviewTest(unittest.TestCase):
    def test_complete_plan_passes_and_records_pinned_addons(self):
        review = CHECK.eks_dependency_review(eks_plan(), True)

        self.assertTrue(review["gate"]["passed"], review["gate"]["failures"])
        self.assertIs(review["bootstrap_self_managed_addons"], False)
        self.assertEqual(
            review["addons"]["vpc-cni"],
            {"placement": "before_compute", "version": "v1.22.4-eksbuild.3"},
        )
        self.assertEqual(review["cni_policy_node_groups"], [NODE_GROUP_MODULE])

    def test_the_failed_2026_09_22_plan_shape_is_rejected(self):
        # Only the Pod Identity agent, with its version left to the module's
        # lookup, which the saved plan could not resolve.
        plan = eks_plan(addons={
            "vpc-cni": None,
            "coredns": None,
            "kube-proxy": None,
            "eks-pod-identity-agent": ("before_compute", None),
        })

        failures = CHECK.eks_dependency_review(plan, True)["gate"]["failures"]

        for name in ("vpc-cni", "coredns", "kube-proxy"):
            self.assertIn(
                f"EKS add-on {name} is missing while self-managed networking "
                "bootstrap is disabled",
                failures,
            )
        self.assertIn(
            "EKS add-on eks-pod-identity-agent has no known version in the saved plan",
            failures,
        )

    def test_cni_that_waits_for_compute_is_rejected(self):
        plan = eks_plan(addons={"vpc-cni": ("this", "v1.22.4-eksbuild.3")})

        failures = CHECK.eks_dependency_review(plan, True)["gate"]["failures"]

        self.assertEqual(len(failures), 1)
        self.assertIn("vpc-cni must be a before_compute add-on", failures[0])

    def test_node_role_without_cni_permission_is_rejected(self):
        failures = CHECK.eks_dependency_review(eks_plan(cni_policy=False), True)[
            "gate"
        ]["failures"]

        self.assertEqual(len(failures), 1)
        self.assertIn("lacks AmazonEKS_CNI_Policy", failures[0])

    def test_unknown_bootstrap_setting_fails_closed(self):
        failures = CHECK.eks_dependency_review(eks_plan(bootstrap=OMIT), True)[
            "gate"
        ]["failures"]

        self.assertIn(
            "cannot confirm how EKS networking is installed: "
            "bootstrap_self_managed_addons is unknown",
            failures,
        )

    def test_self_managed_bootstrap_does_not_require_managed_addons(self):
        plan = eks_plan(
            addons={name: None for name in PINNED_ADDONS}, bootstrap=True
        )

        review = CHECK.eks_dependency_review(plan, True)

        self.assertTrue(review["gate"]["passed"], review["gate"]["failures"])

    def test_unchanged_complete_cluster_passes(self):
        plan = eks_plan()
        plan["resource_changes"] = [
            {
                "address": resource["address"],
                "type": resource["type"],
                "change": {"actions": ["no-op"]},
            }
            for resource in network_addons()
        ]

        self.assertTrue(CHECK.eks_dependency_review(plan, True)["gate"]["passed"])

    def test_cheap_tier_is_not_reviewed(self):
        review = CHECK.eks_dependency_review({"planned_values": {}}, False)

        self.assertFalse(review["required"])
        self.assertTrue(review["gate"]["passed"])


class NodeGroupShapeTest(unittest.TestCase):
    @staticmethod
    def node_group_values(plan):
        node_module = plan["planned_values"]["root_module"]["child_modules"][0]["child_modules"][0]
        return node_module["resources"][0]["values"]

    def test_node_group_matching_the_reported_shape_passes(self):
        runtime = shape(hourly_enabled=True, enable_eks=True)

        self.assertEqual(CHECK.node_group_shape_failures(eks_plan(), runtime), [])

    def test_node_group_edited_alone_is_rejected(self):
        # runtime_shape still reports three workers; apply would create two.
        plan = eks_plan()
        self.node_group_values(plan)["scaling_config"][0]["desired_size"] = 2

        failures = CHECK.node_group_shape_failures(
            plan, shape(hourly_enabled=True, enable_eks=True)
        )

        self.assertEqual(failures, [
            f"{NODE_GROUP_MODULE}.aws_eks_node_group.this[0] plans desired_size 2, "
            "but the reported shape says 3"
        ])

    def test_missing_scaling_or_another_capacity_type_fails_closed(self):
        plan = eks_plan()
        values = self.node_group_values(plan)
        del values["scaling_config"]
        values["capacity_type"] = "ON_DEMAND"

        failures = CHECK.node_group_shape_failures(
            plan, shape(hourly_enabled=True, enable_eks=True)
        )

        self.assertEqual(len(failures), 3, failures)
        for key in ("desired_size None", "max_size None", "capacity_type 'ON_DEMAND'"):
            self.assertTrue(any(key in failure for failure in failures), failures)

    def test_cheap_tier_is_not_compared(self):
        self.assertEqual(CHECK.node_group_shape_failures({}, shape()), [])


class PlanShapeTest(unittest.TestCase):
    def test_project_tag_coverage_checks_nodes_and_volumes_beyond_provider_tags(self):
        def plan():
            return {"planned_values": {"root_module": {"resources": [
                {"address": "aws_eks_cluster.main", "type": "aws_eks_cluster",
                 "values": {"tags_all": {"Project": "my-local-platform"}}},
                {"address": "aws_launch_template.nodes", "type": "aws_launch_template",
                 "values": {
                     "tags_all": {"Project": "my-local-platform"},
                     "tag_specifications": [
                         {"resource_type": kind, "tags": {"Project": "my-local-platform"}}
                         for kind in ("instance", "volume", "network-interface")
                     ],
                 }},
            ]}}}

        self.assertTrue(CHECK.project_tag_coverage(plan(), True)["gate"]["passed"])
        for index in range(3):
            with self.subTest(launch_resource=index):
                changed = plan()
                changed["planned_values"]["root_module"]["resources"][1]["values"]["tag_specifications"][index]["tags"] = {}
                result = CHECK.project_tag_coverage(changed, True)
                self.assertFalse(result["gate"]["passed"])
                self.assertIn("propagated Project tags", result["gate"]["failures"][0])
        changed = plan()
        changed["planned_values"]["root_module"]["resources"][0]["values"]["tags_all"] = {}
        self.assertFalse(CHECK.project_tag_coverage(changed, True)["gate"]["passed"])
        changed["planned_values"]["root_module"]["resources"].pop()
        self.assertIn("EKS requires exactly one reviewed tagged launch template",
                      CHECK.project_tag_coverage(changed, True)["gate"]["failures"])

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
                "kubernetes_version": "1.36",
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
            "m4-aws-account.py",
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
            "eks_version": "1.36",
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
        for module in CHECK.modules(plan_json["planned_values"]["root_module"]):
            for resource in module["resources"]:
                resource["address"] = resource["type"] + ".example"
                resource["values"] = {"tags_all": {"Project": "my-local-platform"}}
        plan_json["planned_values"]["root_module"]["resources"].append({
            "type": "aws_launch_template",
            "address": "aws_launch_template.nodes",
            "values": {"tag_specifications": [
                {"resource_type": kind, "tags": {"Project": "my-local-platform"}}
                for kind in ("instance", "volume", "network-interface")
            ]},
        })
        # The networking prerequisites the fixed EKS topology plans.
        root_module = plan_json["planned_values"]["root_module"]
        root_module["resources"][0]["values"]["bootstrap_self_managed_addons"] = False
        root_module["resources"].extend(network_addons())
        root_module["child_modules"][0]["address"] = NODE_GROUP_MODULE
        root_module["child_modules"][0]["resources"].append(cni_policy_attachment())
        root_module["child_modules"][0]["resources"][0]["values"].update({
            "scaling_config": [{"desired_size": 3, "max_size": 3, "min_size": 1}],
            "capacity_type": "SPOT",
        })
        node_group_drift_plan_json = json.loads(json.dumps(plan_json))
        node_group_drift_plan_json["planned_values"]["root_module"]["child_modules"][0][
            "resources"
        ][0]["values"]["scaling_config"][0]["desired_size"] = 2
        missing_cni_plan_json = json.loads(json.dumps(plan_json))
        missing_cni_plan_json["planned_values"]["root_module"]["resources"] = [
            resource
            for resource in missing_cni_plan_json["planned_values"]["root_module"]["resources"]
            if resource.get("index") != "vpc-cni"
        ]
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
    elif [ "${{MLP_FAKE_MISSING_CNI:-}}" = 1 ]; then
      printf '%s\\n' '{json.dumps(missing_cni_plan_json)}'
    elif [ "${{MLP_FAKE_NODE_GROUP_DRIFT:-}}" = 1 ]; then
      printf '%s\\n' '{json.dumps(node_group_drift_plan_json)}'
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
        aws = '''#!/bin/sh
printf 'aws %s\\n' "$*" >> "$MLP_FAKE_LOG"
case "$1 $2" in
  'sts get-caller-identity') printf '@ACCOUNT@\\n' ;;
  'budgets describe-budget')
    [ "${MLP_FAKE_BUDGET_MISSING:-}" != 1 ] || exit 42
    python3 -c 'import json,os; b=json.loads("""{"BudgetName": "mlp-live-aws-monthly", "BudgetType": "COST", "TimeUnit": "MONTHLY", "BudgetLimit": {"Amount": "5", "Unit": "USD"}, "CostFilters": {"TagKeyValue": ["user:Project$my-local-platform"]}, "CostTypes": {"IncludeTax": false, "IncludeSubscription": true, "UseBlended": false, "IncludeRefund": true, "IncludeCredit": true, "IncludeUpfront": true, "IncludeRecurring": true, "IncludeOtherSubscription": true, "IncludeSupport": true, "IncludeDiscount": true, "UseAmortized": false}}"""); b["BudgetLimit"]["Amount"]=os.environ.get("MLP_FAKE_BUDGET_LIMIT","5.0"); b["CostFilters"]=json.loads(os.environ.get("MLP_FAKE_BUDGET_FILTERS",json.dumps(b["CostFilters"]))); b["CostTypes"]["IncludeTax"]=os.environ.get("MLP_FAKE_INCLUDE_TAX") == "1"; print(json.dumps({"Budget":b}))'
    ;;
  'ce list-cost-allocation-tags')
    printf '{"CostAllocationTags":[{"TagKey":"Project","Type":"UserDefined","Status":"%s"}]}\\n' "${MLP_FAKE_TAG_STATUS:-Active}"
    ;;
  'budgets describe-notifications-for-budget')
    python3 -c 'import json,os; print(json.dumps({"Notifications":[{"NotificationType":kind,"ComparisonOperator":"GREATER_THAN","Threshold":value,"NotificationState":"ALARM" if os.environ.get("MLP_FAKE_BUDGET_ALARM") == "1" else "OK"} for kind,value in [("ACTUAL",80),("ACTUAL",100),("FORECASTED",100)]]}))'
    ;;
  'budgets describe-subscribers-for-notification')
    if [ "${MLP_FAKE_SUBSCRIBER_COUNT:-1}" = 0 ]; then
      printf '%s\\n' '{"Subscribers":[]}'
    else
      printf '%s\\n' '{"Subscribers":[{"SubscriptionType":"EMAIL","Address":"owner@example.invalid"}]}'
    fi
    ;;
  'eks describe-cluster-versions')
    case " $* " in
      *' --cluster-versions '*)
        case " $* " in
          *' --version-status '*|*' --status '*)
            echo 'conflicting EKS version filters' >&2
            exit 254
            ;;
        esac
        ;;
    esac
    [ "${MLP_FAKE_EKS_ERROR:-}" != 1 ] || exit 42
    if [ -n "${MLP_FAKE_EKS_RESPONSE:-}" ]; then
      printf '%s\\n' "$MLP_FAKE_EKS_RESPONSE"
    else
      printf '%s\\n' '{"clusterVersions":[{"clusterVersion":"1.36","versionStatus":"STANDARD_SUPPORT"}]}'
    fi
    ;;
  *) exit 2 ;;
esac
'''.replace("@ACCOUNT@", account_id)
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

    def test_support_rejects_nonstandard_or_unproved_versions_before_plan_and_apply(self):
        versions = (
            {"clusterVersion": "1.36", "versionStatus": "EXTENDED_SUPPORT"},
            {"clusterVersion": "1.36", "versionStatus": "UNSUPPORTED"},
            {"clusterVersion": "1.36"},
            {"clusterVersion": "1.34", "versionStatus": "STANDARD_SUPPORT"},
            {
                "clusterVersion": "1.36",
                "versionStatus": "EXTENDED_SUPPORT",
                "status": "standard-support",
            },
            {
                "clusterVersion": "1.36",
                "versionStatus": "",
                "status": "standard-support",
            },
            {
                "clusterVersion": "1.36",
                "versionStatus": None,
                "status": "standard-support",
            },
        )
        responses = [json.dumps({"clusterVersions": [version]}) for version in versions]
        responses.extend(['{"clusterVersions": []}', '{}', 'not-json'])
        for action in ("plan", "apply"):
            if action == "apply":
                planned = self._run("plan")
                self.assertEqual(planned.returncode, 0, planned.stderr)
                self._write_go_packet()
            for response in responses:
                with self.subTest(action=action, response=response):
                    self.log.write_text("", encoding="utf-8")
                    environment = self.environment.copy()
                    environment["MLP_FAKE_EKS_RESPONSE"] = response
                    if action == "apply":
                        # Each case models a live controller, whose heartbeat
                        # must stay fresh while the EKS responses are exercised.
                        state_path = self.evidence / "controller-state.json"
                        state = json.loads(state_path.read_text(encoding="utf-8"))
                        state["updated_at"] = datetime.now(timezone.utc).strftime(
                            "%Y-%m-%dT%H:%M:%SZ"
                        )
                        state_path.write_text(json.dumps(state), encoding="utf-8")

                    result = self._run(action, environment)

                    self.assertNotEqual(result.returncode, 0)
                    calls = self.log.read_text(encoding="utf-8").splitlines()
                    self.assertTrue(
                        any(call.startswith("aws eks ") for call in calls), result.stderr
                    )
                    self.assertFalse(any(f" {action} " in f" {call} " for call in calls))

    def test_support_accepts_authoritative_status_and_legacy_only_response(self):
        for version in (
            {"clusterVersion": "1.36", "status": "standard-support"},
            {
                "clusterVersion": "1.36",
                "versionStatus": "STANDARD_SUPPORT",
                "status": "extended-support",
            },
        ):
            with self.subTest(version=version):
                environment = self.environment.copy()
                environment["MLP_FAKE_EKS_RESPONSE"] = json.dumps(
                    {"clusterVersions": [version]}
                )

                result = self._run("plan", environment)

                self.assertEqual(result.returncode, 0, result.stderr)

    def test_support_api_error_blocks_plan_and_apply(self):
        for action in ("plan", "apply"):
            with self.subTest(action=action):
                if action == "apply":
                    planned = self._run("plan")
                    self.assertEqual(planned.returncode, 0, planned.stderr)
                    self._write_go_packet()
                self.log.write_text("", encoding="utf-8")
                environment = self.environment.copy()
                environment["MLP_FAKE_EKS_ERROR"] = "1"

                result = self._run(action, environment)

                self.assertNotEqual(result.returncode, 0)
                calls = self.log.read_text(encoding="utf-8").splitlines()
                self.assertFalse(any(f" {action} " in f" {call} " for call in calls))

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
        self.assertIn("describe-budget", result.stderr)
        self.assertIn("failed", result.stderr)
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

    def test_plan_without_the_cni_addon_is_rejected_and_recorded(self):
        environment = self.environment.copy()
        environment["MLP_FAKE_MISSING_CNI"] = "1"

        result = self._run("plan", environment)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("EKS add-on vpc-cni is missing", result.stderr)
        summary = json.loads(self.summary.read_text(encoding="utf-8"))
        self.assertFalse(summary["gate"]["passed"])
        self.assertFalse(summary["eks_dependencies"]["gate"]["passed"])

    def test_node_group_that_differs_from_the_reported_shape_is_rejected(self):
        environment = self.environment.copy()
        environment["MLP_FAKE_NODE_GROUP_DRIFT"] = "1"

        result = self._run("plan", environment)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn(
            "aws_eks_node_group.example plans desired_size 2, but the reported shape says 3",
            result.stderr,
        )
        summary = json.loads(self.summary.read_text(encoding="utf-8"))
        self.assertFalse(summary["gate"]["passed"])
        # The summary GO reads still reports three workers.
        self.assertEqual(summary["shape"]["eks"]["node_desired"], 3)

    def test_complete_eks_plan_records_its_dependencies(self):
        result = self._run("plan")

        self.assertEqual(result.returncode, 0, result.stderr)
        dependencies = json.loads(self.summary.read_text(encoding="utf-8"))[
            "eks_dependencies"
        ]
        self.assertTrue(dependencies["gate"]["passed"])
        self.assertEqual(
            dependencies["addons"]["vpc-cni"]["placement"], "before_compute"
        )

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
        self.assertIn("budget limit is not numeric", result.stderr)
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

    def test_budget_scope_tax_and_activation_drift_block_before_plan(self):
        for key, value in (
            ("MLP_FAKE_BUDGET_FILTERS", "{}"),
            ("MLP_FAKE_BUDGET_FILTERS", '{"TagKeyValue":["user:Project$other"]}'),
            ("MLP_FAKE_INCLUDE_TAX", "1"),
            ("MLP_FAKE_TAG_STATUS", "Inactive"),
        ):
            with self.subTest(key=key, value=value):
                environment = {**self.environment, key: value}
                result = self._run("plan", environment)
                self.assertNotEqual(result.returncode, 0)
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
