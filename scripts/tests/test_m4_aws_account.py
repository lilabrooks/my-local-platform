from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import stat
import tempfile
import unittest
from unittest import mock


SCRIPT = Path(__file__).parents[1] / "m4-aws-account.py"
SPEC = importlib.util.spec_from_file_location("m4_aws_account", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
ACCOUNT_CHECK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ACCOUNT_CHECK)

RUN_ID = "20260908T050000Z"
COMMIT = "a" * 40
ACCOUNT_ID = "123456789012"


class FakeRunner:
    def __init__(self) -> None:
        self.calls: list[list[str]] = []
        self.configured_account = ACCOUNT_ID
        self.eip_limit = 5
        self.spot_limit = 8
        self.rds_azs = ["us-east-1a", "us-east-1b"]
        self.public_access_blocked = True
        self.subscribers = [
            {"SubscriptionType": "EMAIL", "Address": "owner@example.com"}
        ]
        self.notifications = [
            {
                "NotificationType": "ACTUAL",
                "ComparisonOperator": "GREATER_THAN",
                "Threshold": 80.0,
                "ThresholdType": "PERCENTAGE",
            },
            {
                "NotificationType": "FORECASTED",
                "ComparisonOperator": "GREATER_THAN",
                "Threshold": 100.0,
                "ThresholdType": "PERCENTAGE",
            },
        ]
        self.msk_service_quota: dict | None = None
        self.eks_version = {
            "clusterVersion": "1.35",
            "versionStatus": "STANDARD_SUPPORT",
        }
        self.spot_instances: list[dict] = []
        self.spot_requests: list[dict] = []

    def record(self, arguments: list[str]) -> None:
        self.calls.append(arguments)

    def text(self, arguments: list[str]) -> str:
        self.record(arguments)
        if arguments == ["git", "rev-parse", "HEAD"]:
            return COMMIT
        if arguments == [
            "git",
            "status",
            "--porcelain",
            "--untracked-files=all",
        ]:
            return ""
        if arguments[:4] == ["aws", "configure", "get", "sso_account_id"]:
            return self.configured_account
        raise AssertionError(arguments)

    def run(self, arguments: list[str]) -> None:
        self.record(arguments)
        if arguments[:3] != ["aws", "s3api", "head-bucket"]:
            raise AssertionError(arguments)

    def json(self, arguments: list[str]):
        self.record(arguments)
        if arguments[:3] == ["gh", "repo", "view"]:
            return {
                "nameWithOwner": "lilabrooks/my-local-platform",
                "url": "https://github.com/lilabrooks/my-local-platform",
            }
        if arguments[:3] == ["aws", "sts", "get-caller-identity"]:
            return {
                "Account": ACCOUNT_ID,
                "Arn": f"arn:aws:sts::{ACCOUNT_ID}:assumed-role/owner/session",
            }
        if arguments[:3] == ["aws", "s3api", "get-bucket-versioning"]:
            return {"Status": "Enabled"}
        if arguments[:3] == ["aws", "s3api", "get-bucket-encryption"]:
            return {
                "ServerSideEncryptionConfiguration": {
                    "Rules": [
                        {
                            "ApplyServerSideEncryptionByDefault": {
                                "SSEAlgorithm": "AES256"
                            }
                        }
                    ]
                }
            }
        if arguments[:3] == ["aws", "s3api", "get-public-access-block"]:
            return {
                "PublicAccessBlockConfiguration": {
                    "BlockPublicAcls": self.public_access_blocked,
                    "BlockPublicPolicy": self.public_access_blocked,
                    "IgnorePublicAcls": self.public_access_blocked,
                    "RestrictPublicBuckets": self.public_access_blocked,
                }
            }
        if arguments[:3] == ["aws", "budgets", "describe-budget"]:
            return {
                "Budget": {
                    "BudgetName": "mlp-dev-live-runtime",
                    "BudgetType": "COST",
                    "TimeUnit": "MONTHLY",
                    "BudgetLimit": {"Amount": "5.0", "Unit": "USD"},
                }
            }
        if arguments[:3] == [
            "aws",
            "budgets",
            "describe-notifications-for-budget",
        ]:
            return {"Notifications": self.notifications}
        if arguments[:3] == [
            "aws",
            "budgets",
            "describe-subscribers-for-notification",
        ]:
            return {"Subscribers": self.subscribers}
        if arguments[:3] == [
            "aws",
            "service-quotas",
            "list-service-quotas",
        ]:
            service = arguments[arguments.index("--service-code") + 1]
            if service == "kafka":
                return {
                    "Quotas": (
                        [self.msk_service_quota] if self.msk_service_quota else []
                    )
                }
            return {
                "Quotas": [
                    {
                        "QuotaName": "Clusters",
                        "QuotaCode": "L-EKSCLUSTER",
                        "Value": 100.0,
                    }
                ]
            }
        if arguments[:3] == ["aws", "eks", "list-clusters"]:
            return {"clusters": []}
        if arguments[:3] == ["aws", "kafka", "list-clusters-v2"]:
            return {"ClusterInfoList": []}
        if arguments[:3] == ["aws", "rds", "describe-account-attributes"]:
            return {
                "AccountQuotas": [
                    {"AccountQuotaName": "DBInstances", "Max": 40, "Used": 0}
                ]
            }
        if arguments[:3] == [
            "aws",
            "service-quotas",
            "get-service-quota",
        ]:
            code = arguments[arguments.index("--quota-code") + 1]
            if code == "L-34B43A08":
                return {
                    "Quota": {
                        "QuotaName": "All Standard Spot Instance Requests",
                        "QuotaCode": code,
                        "Value": self.spot_limit,
                    }
                }
            if code == "L-0263D0A3":
                return {
                    "Quota": {
                        "QuotaName": "EC2-VPC Elastic IPs",
                        "QuotaCode": code,
                        "Value": self.eip_limit,
                    }
                }
        if arguments[:3] == ["aws", "ec2", "describe-instances"]:
            return {"Reservations": [{"Instances": self.spot_instances}]}
        if arguments[:3] == ["aws", "ec2", "describe-spot-instance-requests"]:
            return {"SpotInstanceRequests": self.spot_requests}
        if arguments[:3] == ["aws", "ec2", "describe-instance-types"]:
            names = arguments[
                arguments.index("--instance-types") + 1 : arguments.index("--output")
            ]
            return {
                "InstanceTypes": [
                    {"InstanceType": name, "VCpuInfo": {"DefaultVCpus": 2}}
                    for name in names
                ]
            }
        if arguments[:3] == ["aws", "ec2", "describe-addresses"]:
            return {"Addresses": []}
        if arguments[:3] == ["aws", "eks", "describe-cluster-versions"]:
            return {"clusterVersions": [self.eks_version]}
        if arguments[:3] == ["aws", "ssm", "get-parameters-by-path"]:
            return {
                "Parameters": [
                    {
                        "Name": "/aws/service/global-infrastructure/services/kafka/regions/us-east-1",
                        "Value": "us-east-1",
                    }
                ]
            }
        if arguments[:3] == [
            "aws",
            "rds",
            "describe-orderable-db-instance-options",
        ]:
            return {
                "OrderableDBInstanceOptions": [
                    {
                        "Engine": "postgres",
                        "EngineVersion": "17.4",
                        "DBInstanceClass": "db.t4g.micro",
                        "StorageType": "gp3",
                        "Vpc": True,
                        "SupportsStorageEncryption": True,
                        "AvailabilityZones": [{"Name": zone} for zone in self.rds_azs],
                    }
                ]
            }
        if arguments[:3] == [
            "aws",
            "ec2",
            "describe-instance-type-offerings",
        ]:
            return {
                "InstanceTypeOfferings": [
                    {
                        "InstanceType": "t3.medium",
                        "LocationType": "availability-zone",
                        "Location": zone,
                    }
                    for zone in ("us-east-1a", "us-east-1b")
                ]
            }
        raise AssertionError(arguments)


class AccountEvidenceTest(unittest.TestCase):
    def test_account_capture_checks_every_gate_without_mutation(self):
        runner = FakeRunner()

        receipt = ACCOUNT_CHECK.collect(
            RUN_ID,
            COMMIT,
            "aws-public-change-feed",
            "us-east-1",
            runner,
        )

        self.assertTrue(receipt["gate"]["passed"])
        self.assertTrue(receipt["aws"]["account_matches_profile"])
        self.assertEqual(receipt["backend"]["versioning"], "Enabled")
        self.assertTrue(receipt["budget"]["has_notification_subscriber"])
        self.assertEqual(len(receipt["quotas"]["items"]), 5)
        self.assertTrue(receipt["availability"]["gate"]["passed"])
        self.assertTrue(
            receipt["availability"]["msk_serverless_region"][
                "documented_region_present"
            ]
        )
        mutating = {
            "create",
            "put",
            "run-instances",
            "allocate-address",
            "request-service-quota-increase",
        }
        self.assertFalse(
            any(any(part in mutating for part in call) for call in runner.calls)
        )

    def test_quota_gate_uses_remaining_capacity(self):
        runner = FakeRunner()
        runner.eip_limit = 0

        receipt = ACCOUNT_CHECK.collect(
            RUN_ID,
            COMMIT,
            "aws-public-change-feed",
            "us-east-1",
            runner,
        )

        self.assertFalse(receipt["quotas"]["gate"]["passed"])
        self.assertEqual(receipt["gate"]["failures"], ["quotas"])
        eip = next(
            item
            for item in receipt["quotas"]["items"]
            if item["quota_code"] == "L-0263D0A3"
        )
        self.assertEqual(eip["remaining"], "0")
        self.assertFalse(eip["passed"])

    def test_spot_quota_subtracts_running_vcpus(self):
        runner = FakeRunner()
        runner.spot_limit = 5
        runner.spot_instances = [{"CpuOptions": {"CoreCount": 2, "ThreadsPerCore": 1}}]
        runner.spot_instances[0]["InstanceId"] = "i-running"

        receipt = ACCOUNT_CHECK.collect(
            RUN_ID,
            COMMIT,
            "aws-public-change-feed",
            "us-east-1",
            runner,
        )

        spot = next(
            item
            for item in receipt["quotas"]["items"]
            if item["quota_code"] == "L-34B43A08"
        )
        self.assertEqual(spot["used"], "2")
        self.assertEqual(spot["remaining"], "3")
        self.assertFalse(spot["passed"])

    def test_spot_quota_counts_unfulfilled_requests(self):
        runner = FakeRunner()
        runner.spot_limit = 6
        runner.spot_requests = [
            {
                "State": "open",
                "LaunchSpecification": {"InstanceType": "t3.medium"},
            }
        ]

        receipt = ACCOUNT_CHECK.collect(
            RUN_ID,
            COMMIT,
            "aws-public-change-feed",
            "us-east-1",
            runner,
        )

        spot = next(
            item
            for item in receipt["quotas"]["items"]
            if item["quota_code"] == "L-34B43A08"
        )
        self.assertEqual(spot["used"], "2")
        self.assertEqual(spot["required"], "6")
        self.assertFalse(spot["passed"])

    def test_account_specific_msk_quota_replaces_the_documented_default(self):
        runner = FakeRunner()
        runner.msk_service_quota = {
            "QuotaName": "Serverless clusters per account",
            "QuotaCode": "L-SERVERLESS",
            "Value": 12,
        }

        receipt = ACCOUNT_CHECK.collect(
            RUN_ID,
            COMMIT,
            "aws-public-change-feed",
            "us-east-1",
            runner,
        )

        msk = next(
            item
            for item in receipt["quotas"]["items"]
            if item["quota_code"] == "L-SERVERLESS"
        )
        self.assertEqual(msk["limit"], "12")
        self.assertEqual(msk["limit_source"], "service-quotas")

    def test_eks_support_accepts_the_deprecated_lowercase_status(self):
        runner = FakeRunner()
        runner.eks_version = {
            "clusterVersion": "1.35",
            "status": "standard-support",
        }

        receipt = ACCOUNT_CHECK.collect(
            RUN_ID,
            COMMIT,
            "aws-public-change-feed",
            "us-east-1",
            runner,
        )

        self.assertTrue(receipt["eks"]["standard_support"])

    def test_budget_notification_drift_is_rejected(self):
        runner = FakeRunner()
        runner.notifications[0]["Threshold"] = 10000

        with self.assertRaisesRegex(ACCOUNT_CHECK.AccountError, "do not match"):
            ACCOUNT_CHECK.collect(
                RUN_ID,
                COMMIT,
                "aws-public-change-feed",
                "us-east-1",
                runner,
            )

    def test_profile_account_mismatch_stops_before_account_queries(self):
        runner = FakeRunner()
        runner.configured_account = "999999999999"

        with self.assertRaisesRegex(ACCOUNT_CHECK.AccountError, "does not match"):
            ACCOUNT_CHECK.collect(
                RUN_ID,
                COMMIT,
                "aws-public-change-feed",
                "us-east-1",
                runner,
            )

        self.assertFalse(any(call[:2] == ["aws", "s3api"] for call in runner.calls))

    def test_backend_and_notification_controls_are_required(self):
        runner = FakeRunner()
        runner.public_access_blocked = False
        with self.assertRaisesRegex(ACCOUNT_CHECK.AccountError, "public access"):
            ACCOUNT_CHECK.collect(
                RUN_ID,
                COMMIT,
                "aws-public-change-feed",
                "us-east-1",
                runner,
            )

        runner = FakeRunner()
        runner.subscribers = []
        with self.assertRaisesRegex(ACCOUNT_CHECK.AccountError, "no notification"):
            ACCOUNT_CHECK.collect(
                RUN_ID,
                COMMIT,
                "aws-public-change-feed",
                "us-east-1",
                runner,
            )

    def test_regional_gate_requires_both_fixed_zones(self):
        runner = FakeRunner()
        runner.rds_azs = ["us-east-1a"]

        receipt = ACCOUNT_CHECK.collect(
            RUN_ID,
            COMMIT,
            "aws-public-change-feed",
            "us-east-1",
            runner,
        )

        self.assertFalse(receipt["availability"]["gate"]["passed"])
        self.assertEqual(receipt["gate"]["failures"], ["availability"])

    def test_preflight_destination_and_output_are_private(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            run = root / ".evidence" / "m4" / RUN_ID
            run.mkdir(parents=True)
            (run / "00-preflight.json").write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "run_id": RUN_ID,
                        "commit": COMMIT,
                        "result": "passed",
                    }
                ),
                encoding="utf-8",
            )
            output = run / "01-identity.txt"

            with mock.patch.object(ACCOUNT_CHECK, "ROOT", root):
                self.assertEqual(
                    ACCOUNT_CHECK.validate_destination(RUN_ID, output), output
                )
                ACCOUNT_CHECK.require_preflight(RUN_ID, COMMIT)
                ACCOUNT_CHECK.write_json(output, {"gate": {"passed": True}})

            self.assertEqual(stat.S_IMODE(os.stat(output).st_mode), 0o600)
            with self.assertRaisesRegex(ACCOUNT_CHECK.AccountError, "directly under"):
                with mock.patch.object(ACCOUNT_CHECK, "ROOT", root):
                    ACCOUNT_CHECK.validate_destination(RUN_ID, root / "01-identity.txt")


if __name__ == "__main__":
    unittest.main()
