from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import stat
import tempfile
import unittest
from unittest import mock


SCRIPT = Path(__file__).parents[1] / "m4-aws-inventory.py"
SPEC = importlib.util.spec_from_file_location("m4_aws_inventory", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
INVENTORY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(INVENTORY)


def tagged() -> list[dict[str, str]]:
    return [{"Key": "Project", "Value": "my-local-platform"}]


class InventoryTest(unittest.TestCase):
    def test_after_inventory_rejects_ecr_while_staging_allows_it(self):
        inventory = {
            "counts": {"ecr": 2},
            "runtime_empty": True,
            "runtime_resources_present": {},
        }
        INVENTORY.require_cleanup_inventory(inventory, Path("04-inventory-before.json"))
        self.assertTrue(inventory["runtime_empty"])
        INVENTORY.require_cleanup_inventory(inventory, Path("21-inventory-after.json"))
        self.assertFalse(inventory["runtime_empty"])
        self.assertEqual(inventory["runtime_resources_present"]["ecr"], 2)

    def responses(self) -> dict[tuple[str, str], dict]:
        return {
            ("resourcegroupstaggingapi", "get-resources"): {
                "ResourceTagMappingList": [{"ResourceARN": "arn:tagged"}]
            },
            ("eks", "list-clusters"): {"clusters": ["mlp-dev", "other"]},
            ("kafka", "list-clusters-v2"): {
                "ClusterInfoList": [
                    {"ClusterName": "mlp-dev-relay"},
                    {"ClusterName": "unrelated"},
                ]
            },
            ("rds", "describe-db-instances"): {
                "DBInstances": [
                    {"DBInstanceIdentifier": "mlp-dev-postgres"},
                    {"DBInstanceIdentifier": "other"},
                ]
            },
            ("ec2", "describe-instances"): {
                "Reservations": [
                    {
                        "Instances": [
                            {
                                "InstanceId": "i-project",
                                "Tags": [
                                    {
                                        "Key": "kubernetes.io/cluster/mlp-dev",
                                        "Value": "owned",
                                    }
                                ],
                            },
                            {"InstanceId": "i-other", "Tags": []},
                        ]
                    }
                ]
            },
            ("ec2", "describe-volumes"): {
                "Volumes": [
                    {
                        "VolumeId": "vol-project",
                        "Tags": [{"Key": "Name", "Value": "mlp-dev-node"}],
                    },
                    {"VolumeId": "vol-other", "Tags": []},
                ]
            },
            ("ec2", "describe-addresses"): {
                "Addresses": [
                    {
                        "AllocationId": "eipalloc-project",
                        "PublicIp": "192.0.2.10",
                        "Tags": tagged(),
                    },
                    {
                        "AllocationId": "eipalloc-other",
                        "PublicIp": "192.0.2.11",
                        "Tags": [],
                    },
                ]
            },
            ("elbv2", "describe-load-balancers"): {
                "LoadBalancers": [
                    {"LoadBalancerName": "mlp-dev-internal"},
                    {"LoadBalancerName": "other"},
                ]
            },
            ("ecr", "describe-repositories"): {
                "repositories": [
                    {"repositoryName": "mlp-dev/relay"},
                    {"repositoryName": "other"},
                ]
            },
            ("ec2", "describe-nat-gateways"): {
                "NatGateways": [
                    {
                        "NatGatewayId": "nat-project",
                        "State": "available",
                        "Tags": tagged(),
                    },
                    {
                        "NatGatewayId": "nat-deleted",
                        "State": "deleted",
                        "Tags": tagged(),
                    },
                ]
            },
            ("logs", "describe-log-groups"): {
                "logGroups": [
                    {"logGroupName": "/aws/eks/mlp-dev/cluster"},
                    {"logGroupName": "/aws/lambda/other"},
                ]
            },
        }

    def test_collect_uses_tags_and_service_native_queries(self):
        responses = self.responses()
        calls: list[list[str]] = []

        def run(arguments: list[str]) -> dict:
            calls.append(arguments)
            return responses[(arguments[0], arguments[1])]

        result = INVENTORY.collect("us-east-1", run)

        self.assertEqual(len(calls), 11)
        self.assertTrue(all("--region" in call for call in calls))
        self.assertEqual(result["counts"]["tagged"], 1)
        self.assertEqual(result["resources"]["eks"], ["mlp-dev"])
        self.assertEqual(result["resources"]["ec2"][0]["InstanceId"], "i-project")
        self.assertEqual(
            result["resources"]["ecr"][0]["repositoryName"], "mlp-dev/relay"
        )

    def test_runtime_gate_counts_every_cleanup_surface_but_not_ecr(self):
        responses = self.responses()
        result = INVENTORY.collect(
            "us-east-1", lambda arguments: responses[(arguments[0], arguments[1])]
        )

        self.assertFalse(result["runtime_empty"])
        self.assertEqual(
            set(result["runtime_resources_present"]),
            {"ebs", "ec2", "eip", "eks", "elb", "logs", "msk", "nat", "rds"},
        )
        self.assertNotIn("ecr", result["runtime_resources_present"])

    def test_empty_runtime_passes_with_cheap_ecr_repositories(self):
        responses = self.responses()
        responses[("eks", "list-clusters")] = {"clusters": []}
        responses[("kafka", "list-clusters-v2")] = {"ClusterInfoList": []}
        responses[("rds", "describe-db-instances")] = {"DBInstances": []}
        responses[("ec2", "describe-instances")] = {"Reservations": []}
        responses[("ec2", "describe-volumes")] = {"Volumes": []}
        responses[("ec2", "describe-addresses")] = {"Addresses": []}
        responses[("elbv2", "describe-load-balancers")] = {"LoadBalancers": []}
        responses[("ec2", "describe-nat-gateways")] = {"NatGateways": []}
        responses[("logs", "describe-log-groups")] = {"logGroups": []}

        result = INVENTORY.collect(
            "us-east-1", lambda arguments: responses[(arguments[0], arguments[1])]
        )

        self.assertTrue(result["runtime_empty"])
        self.assertEqual(result["counts"]["ecr"], 1)

    def test_private_atomic_output_replaces_an_existing_file(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "inventory.json"
            path.write_text("old", encoding="utf-8")

            INVENTORY.write_json(path, {"runtime_empty": True})

            self.assertEqual(
                json.loads(path.read_text(encoding="utf-8")), {"runtime_empty": True}
            )
            mode = stat.S_IMODE(os.stat(path).st_mode)
            self.assertEqual(mode, 0o600)

    def test_destination_must_be_private_and_bound_to_the_run(self):
        run_id = "20260908T030000Z"
        expected = INVENTORY.ROOT / ".evidence" / "m4" / run_id
        self.assertEqual(
            INVENTORY.validate_destination(
                run_id, expected / "04-inventory-before.json"
            ),
            (expected / "04-inventory-before.json").absolute(),
        )

        with self.assertRaisesRegex(INVENTORY.InventoryError, "directly under"):
            INVENTORY.validate_destination(
                run_id, INVENTORY.ROOT / "docs" / "inventory.json"
            )
        with self.assertRaisesRegex(INVENTORY.InventoryError, "real UTC"):
            INVENTORY.validate_destination(
                "20260230T030000Z", expected / "04-inventory-before.json"
            )

    def test_destination_rejects_a_symlinked_run_directory(self):
        run_id = "20260908T030000Z"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "repository"
            outside = Path(temporary) / "outside"
            (root / ".evidence" / "m4").mkdir(parents=True)
            outside.mkdir()
            (root / ".evidence" / "m4" / run_id).symlink_to(outside)

            with mock.patch.object(INVENTORY, "ROOT", root):
                with self.assertRaisesRegex(
                    INVENTORY.InventoryError, "contains a symlink"
                ):
                    INVENTORY.validate_destination(
                        run_id,
                        root / ".evidence" / "m4" / run_id / "04-inventory-before.json",
                    )

    def test_inventory_requires_preflight_for_the_same_commit(self):
        run_id = "20260908T030000Z"
        commit = "a" * 40
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            run = root / ".evidence" / "m4" / run_id
            run.mkdir(parents=True)
            (run / "00-preflight.json").write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "run_id": run_id,
                        "result": "passed",
                        "commit": commit,
                    }
                ),
                encoding="utf-8",
            )

            with mock.patch.object(INVENTORY, "ROOT", root):
                INVENTORY.require_preflight(run_id, commit)
                with self.assertRaisesRegex(
                    INVENTORY.InventoryError, "approved commit"
                ):
                    INVENTORY.require_preflight(run_id, "b" * 40)


if __name__ == "__main__":
    unittest.main()
