from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock


SCRIPT = Path(__file__).parents[1] / "m4-stage.py"
SPEC = importlib.util.spec_from_file_location("m4_stage", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
STAGE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(STAGE)

COMMIT = "a" * 40
RELAY_DIGEST = "sha256:" + "b" * 64
SINK_DIGEST = "sha256:" + "c" * 64
REGISTRY = "123456789012.dkr.ecr.us-east-1.amazonaws.com"
REPOSITORIES = {
    "relay": f"{REGISTRY}/mlp-dev/relay",
    "sink": f"{REGISTRY}/mlp-dev/sink",
}
REVIEWED_RATES = {
    "msk_serverless_cluster_hour": "0.7500",
    "msk_partition_hour": "0.0015",
    "eks_standard_cluster_hour": "0.1000",
    "t3_medium_hour": "0.0416",
    "nat_gateway_hour": "0.0450",
    "public_ipv4_hour": "0.0050",
    "rds_t4g_micro_hour": "0.0160",
    "rds_gp3_gb_month": "0.1150",
}


class FakeRunner:
    def __init__(self, *, existing: bool = False, architecture: str = "amd64"):
        self.existing = existing
        self.architecture = architecture
        self.calls: list[tuple[list[str], bool]] = []

    def record(self, arguments: list[str], input_bytes: bytes | None = None) -> None:
        self.calls.append((arguments, input_bytes is not None))

    def text(self, arguments: list[str]) -> str:
        self.record(arguments)
        if arguments[:3] == ["git", "rev-parse", "HEAD"]:
            return COMMIT
        if arguments[:3] == ["git", "status", "--porcelain"]:
            return ""
        raise AssertionError(arguments)

    def bytes(self, arguments: list[str]) -> bytes:
        self.record(arguments)
        if arguments[:3] == ["aws", "ecr", "get-login-password"]:
            return b"temporary-password\n"
        raise AssertionError(arguments)

    def run(self, arguments: list[str], *, input_bytes: bytes | None = None) -> None:
        self.record(arguments, input_bytes)

    def json(self, arguments: list[str]):
        self.record(arguments)
        if arguments[0] == "terraform":
            return REPOSITORIES
        if arguments[:3] == ["aws", "ecr", "describe-repositories"]:
            return {
                "repositories": [
                    {
                        "repositoryName": f"mlp-dev/{service}",
                        "repositoryUri": uri,
                        "imageTagMutability": "IMMUTABLE",
                        "imageScanningConfiguration": {"scanOnPush": True},
                    }
                    for service, uri in REPOSITORIES.items()
                ]
            }
        if arguments[:3] == ["aws", "ecr", "list-images"]:
            service = arguments[arguments.index("--repository-name") + 1].split("/")[-1]
            digest = RELAY_DIGEST if service == "relay" else SINK_DIGEST
            return {
                "imageIds": (
                    [{"imageTag": COMMIT, "imageDigest": digest}]
                    if self.existing
                    else []
                )
            }
        if arguments[:3] == ["aws", "ecr", "describe-images"]:
            service = arguments[arguments.index("--repository-name") + 1].split("/")[-1]
            digest = RELAY_DIGEST if service == "relay" else SINK_DIGEST
            return {"imageDetails": [{"imageDigest": digest, "imageTags": [COMMIT]}]}
        if arguments[:3] == ["docker", "image", "inspect"]:
            image = arguments[-1]
            digest = SINK_DIGEST if "sink" in image else RELAY_DIGEST
            return {
                "Architecture": self.architecture,
                "Os": "linux",
                "Id": digest,
                "Config": {"Labels": {"org.opencontainers.image.revision": COMMIT}},
            }
        raise AssertionError(arguments)

    def commands(self, prefix: list[str]) -> list[list[str]]:
        return [
            arguments
            for arguments, _ in self.calls
            if arguments[: len(prefix)] == prefix
        ]


class ImageStageTest(unittest.TestCase):
    def test_operator_address_rejects_changed_broad_or_unreachable_ip(self):
        runner = mock.Mock()
        runner.json.return_value = {
            "variables": {"eks_operator_cidr": {"value": "198.51.100.4/32"}}
        }
        response = mock.MagicMock()
        response.__enter__.return_value.read.return_value = b"198.51.100.4\n"
        with mock.patch.object(
            STAGE.urllib.request, "urlopen", return_value=response
        ) as request:
            STAGE.verify_operator_address(Path("saved-plan"), runner)
            response.__enter__.return_value.read.return_value = b"198.51.100.5\n"
            with self.assertRaisesRegex(STAGE.StageError, "changed"):
                STAGE.verify_operator_address(Path("saved-plan"), runner)
            runner.json.return_value["variables"]["eks_operator_cidr"]["value"] = (
                "198.51.100.0/24"
            )
            with self.assertRaises(STAGE.StageError):
                STAGE.verify_operator_address(Path("saved-plan"), runner)
            runner.json.return_value["variables"]["eks_operator_cidr"]["value"] = (
                "198.51.100.4/32"
            )
            request.side_effect = OSError("unreachable")
            with self.assertRaises(STAGE.StageError):
                STAGE.verify_operator_address(Path("saved-plan"), runner)

    def valid_price_input(self, run_id: str, now: datetime) -> dict:
        value = STAGE.price_template(run_id, COMMIT)
        value.update(
            {
                "confirmed": True,
                "checked_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "checked_by": "operator-one",
            }
        )
        value["rates"] = dict(REVIEWED_RATES)
        return value

    def write_release_inputs(self, root: Path, run_id: str) -> Path:
        run = root / ".evidence" / "m4" / run_id
        run.mkdir(parents=True)
        now = datetime.now(timezone.utc).replace(microsecond=0)
        price = STAGE.validate_price_input(
            self.valid_price_input(run_id, now),
            run_id,
            COMMIT,
            "us-east-1",
            now=now,
        )
        expected_counts = {
            "aws_db_instance": 1,
            "aws_eks_cluster": 1,
            "aws_eks_node_group": 1,
            "aws_msk_serverless_cluster": 1,
            "aws_nat_gateway": 1,
        }
        values = {
            "00-preflight.json": {
                "schema_version": 1,
                "run_id": run_id,
                "commit": COMMIT,
                "result": "passed",
            },
            "01-identity.txt": {
                "schema_version": 1,
                "run_id": run_id,
                "source_commit": COMMIT,
                "captured_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "region": "us-east-1",
                "repository": {"name_with_owner": "lilabrooks/my-local-platform"},
                "aws": {
                    "profile": "aws-public-change-feed",
                    "account_id": "123456789012",
                    "account_matches_profile": True,
                },
                "backend": {
                    "bucket": "mlp-tfstate-123456789012",
                    "exists": True,
                    "versioning": "Enabled",
                    "encryption": "AES256",
                    "public_access_blocked": True,
                },
                "eks": {"standard_support": True},
                "budget": {
                    "active": True,
                    "limit_usd": "5.00",
                    "has_notification_subscriber": True,
                },
                "quotas": {"gate": {"passed": True, "failures": []}},
                "availability": {"gate": {"passed": True, "failures": []}},
                "gate": {"passed": True, "failures": []},
            },
            "02-prices.json": price,
            "03-plan-summary.json": {
                "schema_version": 1,
                "run_id": run_id,
                "source_commit": COMMIT,
                "captured_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "plan_sha256": "d" * 64,
                "shape": {
                    "region": "us-east-1",
                    "hourly_enabled": True,
                    "enable_eks": True,
                    "enable_msk": True,
                    "enable_rds": True,
                    "expected_hourly_usd": price["total_hourly_usd"],
                    "maximum_hourly_usd": 1.25,
                    "eks": {
                        "kubernetes_version": "1.35",
                        "node_capacity_type": "SPOT",
                        "node_desired": 2,
                        "node_maximum": 3,
                    },
                    "kafka": {
                        "delivery_topic": "mlp.relay.deliveries",
                        "delivery_partitions": 12,
                        "dead_letter_topic": "mlp.relay.deliveries.dlq",
                        "dead_letter_partitions": 1,
                        "total_partitions": 13,
                    },
                },
                "planned_resource_count": 40,
                "created_resource_count": 38,
                "planned_hourly_resource_counts": expected_counts,
                "created_hourly_resource_counts": expected_counts,
                "gate": {"passed": True, "failures": []},
            },
            "04-inventory-before.json": {
                "schema_version": 1,
                "run_id": run_id,
                "source_commit": COMMIT,
                "captured_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "region": "us-east-1",
                "runtime_empty": True,
                "counts": {"ecr": 2},
            },
            "05-images.json": {
                "schema_version": 1,
                "run_id": run_id,
                "source_commit": COMMIT,
                "region": "us-east-1",
                "images": {
                    "relay": {
                        "revision": COMMIT,
                        "platform": "linux/amd64",
                        "digest": RELAY_DIGEST,
                        "reference": f"{REPOSITORIES['relay']}@{RELAY_DIGEST}",
                    },
                    "sink": {
                        "revision": COMMIT,
                        "platform": "linux/amd64",
                        "digest": SINK_DIGEST,
                        "reference": f"{REPOSITORIES['sink']}@{SINK_DIGEST}",
                    },
                },
                "repository_settings": {
                    "relay": {
                        "repository": "mlp-dev/relay",
                        "uri": REPOSITORIES["relay"],
                        "tag_mutability": "IMMUTABLE",
                        "scan_on_push": True,
                    },
                    "sink": {
                        "repository": "mlp-dev/sink",
                        "uri": REPOSITORIES["sink"],
                        "tag_mutability": "IMMUTABLE",
                        "scan_on_push": True,
                    },
                },
                "gate": {"passed": True, "failures": []},
            },
            "capture-plan.json": {
                "schema_version": 1,
                "run_id": run_id,
                "commit": COMMIT,
                "paid_window": {
                    "evidence_deadline_minutes": 150,
                    "hard_deadline_minutes": 180,
                    "destroy_starts_after_success_or_failure": True,
                },
                "failure_transition": {"next_phase": "destroy_first"},
                "captures": [
                    {
                        "order": 6,
                        "phase": "before_paid_window",
                        "output": "06-go-no-go.json",
                    },
                    {
                        "order": 7,
                        "phase": "before_paid_window",
                        "output": "00-session.json",
                    },
                    {"order": 10, "phase": "live_window", "output": "10-event.json"},
                    {"order": 20, "phase": "destroy_first", "output": "20-destroy.txt"},
                ],
            },
        }
        for name, value in values.items():
            (run / name).write_text(json.dumps(value), encoding="utf-8")
        (run / "02-prices.md").write_text(STAGE.price_markdown(price), encoding="utf-8")
        return run

    def test_new_images_are_pushed_once_and_recorded_by_digest(self):
        runner = FakeRunner()

        receipt = STAGE.capture_images(COMMIT, "us-east-1", True, runner)

        self.assertEqual(len(runner.commands(["docker", "image", "tag"])), 2)
        self.assertEqual(len(runner.commands(["docker", "image", "push"])), 2)
        self.assertEqual(receipt["images"]["relay"]["digest"], RELAY_DIGEST)
        self.assertEqual(
            receipt["images"]["relay"]["reference"],
            f"{REPOSITORIES['relay']}@{RELAY_DIGEST}",
        )
        self.assertTrue(receipt["images"]["relay"]["pushed"])
        login = [call for call in runner.calls if call[0][:2] == ["docker", "login"]]
        self.assertEqual(len(login), 1)
        self.assertTrue(login[0][1])

    def test_existing_immutable_tags_are_pulled_and_verified(self):
        runner = FakeRunner(existing=True)

        receipt = STAGE.capture_images(COMMIT, "us-east-1", True, runner)

        self.assertEqual(runner.commands(["docker", "image", "tag"]), [])
        self.assertEqual(runner.commands(["docker", "image", "push"]), [])
        self.assertEqual(len(runner.commands(["docker", "image", "pull"])), 2)
        self.assertTrue(
            all(
                "linux/amd64" in command
                for command in runner.commands(["docker", "image", "pull"])
            )
        )
        self.assertFalse(receipt["images"]["sink"]["pushed"])

    def test_inspection_refuses_a_missing_tag(self):
        runner = FakeRunner()

        with self.assertRaisesRegex(STAGE.StageError, "image is missing"):
            STAGE.capture_images(COMMIT, "us-east-1", False, runner)

        self.assertEqual(runner.commands(["docker", "image", "push"]), [])

    def test_wrong_platform_fails_before_push(self):
        runner = FakeRunner(architecture="arm64")

        with self.assertRaisesRegex(STAGE.StageError, "expected linux/amd64"):
            STAGE.capture_images(COMMIT, "us-east-1", True, runner)

        self.assertEqual(runner.commands(["docker", "image", "push"]), [])

    def test_repository_settings_must_be_immutable_and_scan_on_push(self):
        value = {
            "repositories": [
                {
                    "repositoryName": "mlp-dev/relay",
                    "repositoryUri": REPOSITORIES["relay"],
                    "imageTagMutability": "MUTABLE",
                    "imageScanningConfiguration": {"scanOnPush": True},
                },
                {
                    "repositoryName": "mlp-dev/sink",
                    "repositoryUri": REPOSITORIES["sink"],
                    "imageTagMutability": "IMMUTABLE",
                    "imageScanningConfiguration": {"scanOnPush": True},
                },
            ]
        }

        with self.assertRaisesRegex(STAGE.StageError, "immutable tags"):
            STAGE.validate_repository_settings(REPOSITORIES, value)

    def test_repository_urls_are_bound_to_service_region_and_registry(self):
        runner = FakeRunner()
        self.assertEqual(STAGE.repositories("us-east-1", runner), REPOSITORIES)

        wrong = FakeRunner()
        original = wrong.json

        def wrong_region(arguments: list[str]):
            if arguments[0] == "terraform":
                return {
                    **REPOSITORIES,
                    "sink": REPOSITORIES["sink"].replace("us-east-1", "us-west-2"),
                }
            return original(arguments)

        wrong.json = wrong_region
        with self.assertRaisesRegex(STAGE.StageError, "does not match"):
            STAGE.repositories("us-east-1", wrong)

    def test_preflight_and_destination_are_bound_to_the_same_run(self):
        run_id = "20260908T040000Z"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            run = root / ".evidence" / "m4" / run_id
            run.mkdir(parents=True)
            output = run / "05-images.json"
            (run / "00-preflight.json").write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "run_id": run_id,
                        "result": "passed",
                        "commit": COMMIT,
                    }
                ),
                encoding="utf-8",
            )

            with mock.patch.object(STAGE, "ROOT", root):
                self.assertEqual(STAGE.ensure_private_run_path(run_id, output), output)
                STAGE.require_preflight(run_id, COMMIT)
                with self.assertRaisesRegex(STAGE.StageError, "does not match"):
                    STAGE.require_preflight(run_id, "d" * 40)
                with self.assertRaisesRegex(STAGE.StageError, "directly under"):
                    STAGE.ensure_private_run_path(run_id, root / "05-images.json")

    def test_current_price_input_recomputes_the_fixed_topology(self):
        run_id = "20260908T040000Z"
        now = datetime(2026, 9, 8, 4, 0, tzinfo=timezone.utc)

        receipt = STAGE.validate_price_input(
            self.valid_price_input(run_id, now),
            run_id,
            COMMIT,
            "us-east-1",
            now=now,
        )

        self.assertEqual(
            receipt["total_hourly_usd"],
            "1.021850684931506849315068493",
        )
        self.assertTrue(receipt["gate"]["passed"])
        markdown = STAGE.price_markdown(receipt)
        self.assertIn("$1.0219/hour", markdown)
        self.assertIn("https://aws.amazon.com/msk/pricing/", markdown)

    def test_price_input_requires_an_explicit_fresh_review(self):
        run_id = "20260908T040000Z"
        now = datetime(2026, 9, 8, 4, 0, tzinfo=timezone.utc)
        value = self.valid_price_input(run_id, now)
        value["confirmed"] = False
        with self.assertRaisesRegex(STAGE.StageError, "must be confirmed"):
            STAGE.validate_price_input(value, run_id, COMMIT, "us-east-1", now=now)

        value = self.valid_price_input(run_id, now)
        value["checked_at"] = "2026-09-07T03:59:59Z"
        with self.assertRaisesRegex(STAGE.StageError, "older than 24 hours"):
            STAGE.validate_price_input(value, run_id, COMMIT, "us-east-1", now=now)

    def test_price_gate_rejects_a_total_above_the_contract(self):
        run_id = "20260908T040000Z"
        now = datetime(2026, 9, 8, 4, 0, tzinfo=timezone.utc)
        value = self.valid_price_input(run_id, now)
        value["rates"]["msk_serverless_cluster_hour"] = "1.2500"

        with self.assertRaisesRegex(STAGE.StageError, "exceeds the 1.25"):
            STAGE.validate_price_input(value, run_id, COMMIT, "us-east-1", now=now)

    def test_price_templates_do_not_share_mutable_rates(self):
        first = STAGE.price_template("20260908T040000Z", COMMIT)
        first["rates"]["msk_serverless_cluster_hour"] = "99"

        second = STAGE.price_template("20260908T040001Z", COMMIT)

        self.assertEqual(second["rates"]["msk_serverless_cluster_hour"], "")

    def test_price_template_does_not_prefill_reviewed_rates(self):
        value = STAGE.price_template("20260908T040000Z", COMMIT)

        self.assertEqual(set(value["rates"]), set(REVIEWED_RATES))
        self.assertTrue(all(rate == "" for rate in value["rates"].values()))

    def test_go_packet_binds_every_gate_to_one_run_and_commit(self):
        run_id = "20260908T040000Z"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.write_release_inputs(root, run_id)

            with mock.patch.object(STAGE, "ROOT", root):
                receipt = STAGE.build_go_no_go(
                    run_id,
                    COMMIT,
                    "us-east-1",
                    "operator-one",
                    FakeRunner(),
                )

        self.assertEqual(receipt["decision"], "go")
        self.assertEqual(receipt["source_commit"], COMMIT)
        self.assertEqual(receipt["cleanup_owner"], "operator-one")
        self.assertEqual(receipt["abort_command"], "make aws-down")
        self.assertEqual(len(receipt["input_sha256"]), 8)
        self.assertEqual(
            receipt["image_references"]["relay"],
            f"{REPOSITORIES['relay']}@{RELAY_DIGEST}",
        )

    def test_go_packet_rejects_mixed_commit_evidence(self):
        run_id = "20260908T040000Z"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            run = self.write_release_inputs(root, run_id)
            inventory_path = run / "04-inventory-before.json"
            inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
            inventory["source_commit"] = "d" * 40
            inventory_path.write_text(json.dumps(inventory), encoding="utf-8")

            with mock.patch.object(STAGE, "ROOT", root):
                with self.assertRaisesRegex(STAGE.StageError, "approved commit"):
                    STAGE.build_go_no_go(
                        run_id,
                        COMMIT,
                        "us-east-1",
                        "operator-one",
                        FakeRunner(),
                    )

    def test_go_packet_rejects_stale_inventory(self):
        run_id = "20260908T040000Z"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            run = self.write_release_inputs(root, run_id)
            inventory_path = run / "04-inventory-before.json"
            inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
            inventory["captured_at"] = "2000-01-01T00:00:00Z"
            inventory_path.write_text(json.dumps(inventory), encoding="utf-8")

            with mock.patch.object(STAGE, "ROOT", root):
                with self.assertRaisesRegex(STAGE.StageError, "older than 24 hours"):
                    STAGE.build_go_no_go(
                        run_id,
                        COMMIT,
                        "us-east-1",
                        "operator-one",
                        FakeRunner(),
                    )

    def test_go_packet_recomputes_price_evidence(self):
        run_id = "20260908T040000Z"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            run = self.write_release_inputs(root, run_id)
            price_path = run / "02-prices.json"
            price = json.loads(price_path.read_text(encoding="utf-8"))
            price["total_hourly_usd"] = "0.01"
            price_path.write_text(json.dumps(price), encoding="utf-8")

            with mock.patch.object(STAGE, "ROOT", root):
                with self.assertRaisesRegex(STAGE.StageError, "recomputed"):
                    STAGE.build_go_no_go(
                        run_id,
                        COMMIT,
                        "us-east-1",
                        "operator-one",
                        FakeRunner(),
                    )

    def test_go_packet_rejects_weakened_state_backend(self):
        run_id = "20260908T040000Z"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            run = self.write_release_inputs(root, run_id)
            identity_path = run / "01-identity.txt"
            identity = json.loads(identity_path.read_text(encoding="utf-8"))
            identity["backend"]["versioning"] = "Suspended"
            identity_path.write_text(json.dumps(identity), encoding="utf-8")

            with mock.patch.object(STAGE, "ROOT", root):
                with self.assertRaisesRegex(STAGE.StageError, "backend controls"):
                    STAGE.build_go_no_go(
                        run_id,
                        COMMIT,
                        "us-east-1",
                        "operator-one",
                        FakeRunner(),
                    )

    def test_go_packet_rejects_an_image_registry_from_another_account(self):
        run_id = "20260908T040000Z"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            run = self.write_release_inputs(root, run_id)
            identity_path = run / "01-identity.txt"
            identity = json.loads(identity_path.read_text(encoding="utf-8"))
            identity["aws"]["account_id"] = "210987654321"
            identity["backend"]["bucket"] = "mlp-tfstate-210987654321"
            identity_path.write_text(json.dumps(identity), encoding="utf-8")

            with mock.patch.object(STAGE, "ROOT", root):
                with self.assertRaisesRegex(STAGE.StageError, "verified AWS account"):
                    STAGE.build_go_no_go(
                        run_id,
                        COMMIT,
                        "us-east-1",
                        "operator-one",
                        FakeRunner(),
                    )

    def test_apply_boundary_verifies_go_packet_plan_and_summary(self):
        run_id = "20260908T040000Z"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            run = self.write_release_inputs(root, run_id)
            plan_path = run / "reviewed.tfplan"
            plan_path.write_bytes(b"reviewed plan")
            summary_path = run / "03-plan-summary.json"
            summary = json.loads(summary_path.read_text())
            summary["plan_sha256"] = STAGE.hash_file(plan_path)
            summary_path.write_text(json.dumps(summary))

            with mock.patch.object(STAGE, "ROOT", root):
                packet = STAGE.build_go_no_go(
                    run_id,
                    COMMIT,
                    "us-east-1",
                    "operator-one",
                    FakeRunner(),
                )
                packet["plan"]["sha256"] = STAGE.hash_file(plan_path)
                packet["input_sha256"]["plan"] = STAGE.hash_file(summary_path)
                packet_path = run / "06-go-no-go.json"
                packet_path.write_text(json.dumps(packet), encoding="utf-8")
                STAGE.verify_go_no_go(
                    run_id,
                    COMMIT,
                    "us-east-1",
                    packet_path,
                    plan_path,
                    summary_path,
                )

                identity = run / "01-identity.txt"
                original = identity.read_bytes()
                identity.write_bytes(original + b"\n")
                with self.assertRaisesRegex(
                    STAGE.StageError, "staged identity changed"
                ):
                    STAGE.verify_go_no_go(
                        run_id,
                        COMMIT,
                        "us-east-1",
                        packet_path,
                        plan_path,
                        summary_path,
                    )
                identity.write_bytes(original)

                from datetime import timedelta

                now = datetime.now(timezone.utc)
                near_expiry = json.loads(original)
                near_expiry["captured_at"] = (
                    now - timedelta(hours=23, minutes=55)
                ).strftime("%Y-%m-%dT%H:%M:%SZ")
                identity.write_text(json.dumps(near_expiry))
                packet["input_sha256"]["identity"] = STAGE.hash_file(identity)
                packet_path.write_text(json.dumps(packet))
                STAGE.verify_go_no_go(
                    run_id,
                    COMMIT,
                    "us-east-1",
                    packet_path,
                    plan_path,
                    summary_path,
                    now=now,
                )
                with self.assertRaisesRegex(
                    STAGE.StageError, "original identity observation"
                ):
                    STAGE.verify_go_no_go(
                        run_id,
                        COMMIT,
                        "us-east-1",
                        packet_path,
                        plan_path,
                        summary_path,
                        now=now,
                        before_session=True,
                    )
                identity.write_bytes(original)
                packet["input_sha256"]["identity"] = STAGE.hash_file(identity)

                future = datetime.now(timezone.utc) + timedelta(hours=25)
                packet["generated_at"] = future.strftime("%Y-%m-%dT%H:%M:%SZ")
                packet_path.write_text(json.dumps(packet))
                with self.assertRaisesRegex(
                    STAGE.StageError, "original identity observation"
                ):
                    STAGE.verify_go_no_go(
                        run_id,
                        COMMIT,
                        "us-east-1",
                        packet_path,
                        plan_path,
                        summary_path,
                        now=future,
                    )
                packet["generated_at"] = datetime.now(timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                )
                packet_path.write_text(json.dumps(packet))

                plan_path.write_bytes(b"replaced plan")
                with self.assertRaisesRegex(STAGE.StageError, "reviewed plan"):
                    STAGE.verify_go_no_go(
                        run_id,
                        COMMIT,
                        "us-east-1",
                        packet_path,
                        plan_path,
                        summary_path,
                    )


if __name__ == "__main__":
    unittest.main()
