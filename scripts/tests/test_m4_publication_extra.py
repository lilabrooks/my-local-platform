from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import scripts.tests.test_m4_evidence as evidence_fixture
import scripts.tests.test_m4_stage as stage_fixture

SPEC = importlib.util.spec_from_file_location(
    "extra", Path(__file__).parents[1] / "m4-publication-extra.py"
)
EXTRA = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(EXTRA)
RUN = evidence_fixture.RUN_ID
OBS = "20260911T120000Z"


class ExtraPublicationTests(unittest.TestCase):
    def failed_run(self, root):
        fixture = evidence_fixture.M4EvidenceTest()
        raw = fixture.create_run(root)
        redactions = fixture.write_packet(raw)
        (raw / "06-go-no-go.json").write_text(
            json.dumps({"aws_profile": "fixture-profile"})
        )
        state = json.loads((raw / "controller-state.json").read_text())
        state.update(
            phase="cleanup_failed",
            cleanup_verified=False,
            cleanup_blocked_reason="identity_unverified",
            controller_pid=4321,
        )
        (raw / "controller-state.json").write_text(json.dumps(state))
        EXTRA.E.publish(root, RUN, "failed", redactions)
        backend = root / "infra/terraform/envs/dev/.terraform/terraform.tfstate"
        backend.parent.mkdir(parents=True)
        binding = EXTRA.expected_backend("123456789012")
        backend.write_text(
            json.dumps(
                {
                    "backend": {
                        "type": "s3",
                        "config": {
                            k: v
                            for k, v in binding.items()
                            if k not in {"type", "workspace"}
                        },
                    }
                }
            )
        )
        return raw, redactions

    def empty_inventory(self):
        services = set(EXTRA.INVENTORY.RUNTIME_SERVICES) | {"ecr"}
        return {
            "schema_version": 1,
            "region": "us-east-1",
            "project": "my-local-platform",
            "captured_at": EXTRA.utc(),
            "counts": {k: 0 for k in services},
            "resources": {k: [] for k in services},
            "runtime_empty": True,
        }

    def routed_inventory(self, region, *, runner):
        self.assertEqual(region, "us-east-1")
        runner(["ec2", "describe-instances"])
        return self.empty_inventory()

    def test_recovery_requires_both_empty_checks_and_preserves_failed_history(self):
        for state_exit, inventory_failure in ((0, False), (1, False), (0, True)):
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                raw, redactions = self.failed_run(root)
                original = (
                    root / "docs/evidence/m4" / RUN / "publication.json"
                ).read_bytes()
                # A later refresh must not change the original recovery target.
                (raw / "01-identity.txt").write_text(
                    json.dumps({"aws": {"account_id": "999999999999"}})
                )
                (raw / "06-go-no-go.json").write_text(
                    json.dumps({"aws_profile": "different-profile"})
                )
                with (
                    mock.patch.object(EXTRA, "require_quiet_controller"),
                    mock.patch.object(
                        EXTRA.E,
                        "private_aws_json",
                        return_value={"Account": "123456789012"},
                    ) as aws,
                    mock.patch.object(
                        EXTRA.subprocess,
                        "run",
                        return_value=mock.Mock(returncode=state_exit, stdout=b""),
                    ) as command,
                    mock.patch.object(
                        EXTRA.INVENTORY,
                        "collect",
                        side_effect=EXTRA.INVENTORY.InventoryError("unavailable")
                        if inventory_failure
                        else self.routed_inventory,
                    ),
                ):
                    summary = EXTRA.collect_recovery(root, RUN, OBS, redactions)
                self.assertTrue(
                    all(
                        call.args[0] == raw / "failed-publication"
                        for call in aws.call_args_list
                    )
                )
                self.assertEqual(
                    command.call_args.kwargs["env"]["AWS_PROFILE"], "fixture-profile"
                )
                if not inventory_failure:
                    self.assertEqual(
                        aws.call_args.args[1], ["ec2", "describe-instances"]
                    )
                self.assertEqual(
                    summary["cleanup_verified"],
                    state_exit == 0 and not inventory_failure,
                )
                self.assertFalse(summary["demonstration_passed"])
                target = EXTRA.publish_recovery(root, RUN, OBS, redactions)
                EXTRA.publish_recovery(root, RUN, OBS, redactions, verify=True)
                self.assertEqual(
                    (root / "docs/evidence/m4" / RUN / "publication.json").read_bytes(),
                    original,
                )
                self.assertEqual(len(list(target.iterdir())), 2)
                with self.assertRaises(EXTRA.E.EvidenceError):
                    EXTRA.publish_recovery(root, RUN, OBS, redactions)
                (raw / "controller-state.json").write_text("{}")
                EXTRA.publish_recovery(root, RUN, OBS, redactions, verify=True)

    def test_wrong_recovery_account_skips_state_and_inventory(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, redactions = self.failed_run(root)
            with (
                mock.patch.object(EXTRA, "require_quiet_controller"),
                mock.patch.object(
                    EXTRA.E,
                    "private_aws_json",
                    return_value={"Account": "999999999999"},
                ),
                mock.patch.object(EXTRA.subprocess, "run") as command,
                mock.patch.object(EXTRA.INVENTORY, "collect") as inventory,
            ):
                summary = EXTRA.collect_recovery(root, RUN, OBS, redactions)
            self.assertFalse(summary["cleanup_verified"])
            command.assert_not_called()
            inventory.assert_not_called()

    def test_wrong_backend_skips_state_and_inventory(self):
        for override in (
            {"key": "other/state"},
            {"endpoint": "https://other.invalid"},
            {"bucket": "other"},
        ):
            with self.subTest(override=override), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                _, redactions = self.failed_run(root)
                metadata = (
                    root / "infra/terraform/envs/dev/.terraform/terraform.tfstate"
                )
                value = json.loads(metadata.read_text())
                value["backend"]["config"].update(override)
                metadata.write_text(json.dumps(value))
                with (
                    mock.patch.object(EXTRA, "require_quiet_controller"),
                    mock.patch.object(
                        EXTRA.E,
                        "private_aws_json",
                        return_value={"Account": "123456789012"},
                    ),
                    mock.patch.object(EXTRA.subprocess, "run") as command,
                    mock.patch.object(EXTRA.INVENTORY, "collect") as inventory,
                ):
                    summary = EXTRA.collect_recovery(root, RUN, OBS, redactions)
                self.assertFalse(summary["cleanup_verified"])
                command.assert_not_called()
                inventory.assert_not_called()

    def test_recovery_rejects_wrong_inventory_scope_and_pre_failure_capture(self):
        for change in (
            {"region": "eu-west-1"},
            {"project": "other"},
            {"schema_version": True},
            {"captured_at": "2026-09-01T00:00:00Z"},
        ):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                raw, redactions = self.failed_run(root)
                with (
                    mock.patch.object(EXTRA, "require_quiet_controller"),
                    mock.patch.object(
                        EXTRA.E,
                        "private_aws_json",
                        return_value={"Account": "123456789012"},
                    ),
                    mock.patch.object(
                        EXTRA.subprocess,
                        "run",
                        return_value=mock.Mock(returncode=0, stdout=b""),
                    ) as command,
                    mock.patch.object(
                        EXTRA.INVENTORY,
                        "collect",
                        side_effect=lambda *args, **kwargs: self.empty_inventory(),
                    ),
                ):
                    self.assertTrue(
                        EXTRA.collect_recovery(root, RUN, OBS, redactions)[
                            "cleanup_verified"
                        ]
                    )
                self.assertEqual(
                    command.call_args.kwargs["env"]["TF_WORKSPACE"], "default"
                )
                path = raw / "recovery" / OBS
                inventory = json.loads((path / "inventory.json").read_text())
                inventory.update(change)
                (path / "inventory.json").write_text(json.dumps(inventory))
                record = json.loads((path / "recovery-result.json").read_text())
                record["files"]["inventory.json"] = EXTRA.E.hash_file(
                    path / "inventory.json"
                )
                (path / "recovery-result.json").write_text(json.dumps(record))
                if "captured_at" in change:
                    with self.assertRaises(EXTRA.E.EvidenceError):
                        EXTRA.recovery_summary(root, RUN, OBS)
                else:
                    self.assertFalse(
                        EXTRA.recovery_summary(root, RUN, OBS)["cleanup_verified"]
                    )

    def staging_packet(self, root):
        fixture = stage_fixture.ImageStageTest()
        raw = fixture.write_release_inputs(root, RUN)
        plan = root / "infra/terraform/envs/dev/.terraform/mlp-reviewed.tfplan"
        plan.parent.mkdir(parents=True)
        plan.write_bytes(b"fixture plan")
        summary = json.loads((raw / "03-plan-summary.json").read_text())
        summary["plan_sha256"] = EXTRA.E.hash_file(plan)
        (raw / "03-plan-summary.json").write_text(json.dumps(summary))
        with mock.patch.object(stage_fixture.STAGE, "ROOT", root):
            packet = stage_fixture.STAGE.build_go_no_go(
                RUN,
                stage_fixture.COMMIT,
                "us-east-1",
                "owner",
                stage_fixture.FakeRunner(),
            )
        (raw / "06-go-no-go.json").write_text(json.dumps(packet))
        redactions = raw / "redactions.json"
        redactions.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "values": {"ACCOUNT_ID": "123456789012", "OPERATOR": "owner"},
                }
            )
        )
        return raw, plan, packet, redactions

    def test_historical_staging_survives_missing_or_replaced_shared_plan(self):
        for state in ("present", "missing", "replaced"):
            with (
                self.subTest(state=state),
                tempfile.TemporaryDirectory() as tmp,
                tempfile.TemporaryDirectory() as out,
            ):
                root, output = Path(tmp), Path(out)
                raw, plan, packet, redactions = self.staging_packet(root)
                if state == "missing":
                    plan.unlink()
                elif state == "replaced":
                    plan.write_bytes(b"later run plan")
                target = EXTRA.publish_staging(root, RUN, redactions, output)
                receipt = json.loads((target / "publication.json").read_text())
                self.assertFalse(receipt["execution_authorized"])
                EXTRA.verify_staging(
                    root, RUN, EXTRA.E.load_redactions(redactions), output
                )
                # The historical path must not weaken the actual execution gate.
                with mock.patch.object(EXTRA.STAGE, "ROOT", root):
                    args = (
                        RUN,
                        packet["source_commit"],
                        "us-east-1",
                        raw / "06-go-no-go.json",
                        plan,
                        raw / "03-plan-summary.json",
                    )
                    if state == "present":
                        EXTRA.STAGE.verify_go_no_go(*args)
                    else:
                        with self.assertRaises(
                            (FileNotFoundError, EXTRA.STAGE.StageError)
                        ):
                            EXTRA.STAGE.verify_go_no_go(*args)

    def test_historical_staging_rejects_changed_inputs_and_plan_hash_binding(self):
        for mutation in ("input", "summary", "malformed"):
            with (
                self.subTest(mutation=mutation),
                tempfile.TemporaryDirectory() as tmp,
                tempfile.TemporaryDirectory() as out,
            ):
                root = Path(tmp)
                raw, plan, packet, redactions = self.staging_packet(root)
                plan.unlink()
                if mutation == "input":
                    (raw / "05-images.json").write_text("{}")
                elif mutation == "summary":
                    summary = json.loads((raw / "03-plan-summary.json").read_text())
                    summary["plan_sha256"] = "f" * 64
                    (raw / "03-plan-summary.json").write_text(json.dumps(summary))
                    packet["input_sha256"]["plan"] = EXTRA.E.hash_file(
                        raw / "03-plan-summary.json"
                    )
                else:
                    packet["plan"]["sha256"] = "not-a-hash"
                (raw / "06-go-no-go.json").write_text(json.dumps(packet))
                with self.assertRaises(EXTRA.STAGE.StageError):
                    EXTRA.publish_staging(root, RUN, redactions, Path(out))
                self.assertFalse(
                    (Path(out) / "docs/evidence/m4-staging" / RUN).exists()
                )

    def test_staging_packet_is_independent_and_requires_separate_checkout(self):
        fixture = stage_fixture.ImageStageTest()
        with (
            tempfile.TemporaryDirectory() as tmp,
            tempfile.TemporaryDirectory() as output,
        ):
            root, output_root = Path(tmp), Path(output)
            raw = fixture.write_release_inputs(root, RUN)
            plan = root / "infra/terraform/envs/dev/.terraform/mlp-reviewed.tfplan"
            plan.parent.mkdir(parents=True)
            plan.write_bytes(b"fixture plan")
            summary = json.loads((raw / "03-plan-summary.json").read_text())
            summary["plan_sha256"] = EXTRA.E.hash_file(plan)
            (raw / "03-plan-summary.json").write_text(json.dumps(summary))
            with mock.patch.object(stage_fixture.STAGE, "ROOT", root):
                packet = stage_fixture.STAGE.build_go_no_go(
                    RUN,
                    stage_fixture.COMMIT,
                    "us-east-1",
                    "owner",
                    stage_fixture.FakeRunner(),
                )
            packet["plan"]["sha256"] = EXTRA.E.hash_file(plan)
            (raw / "06-go-no-go.json").write_text(json.dumps(packet))
            redactions = raw / "redactions.json"
            redactions.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "values": {"ACCOUNT_ID": "123456789012", "OPERATOR": "owner"},
                    }
                )
            )
            with self.assertRaisesRegex(EXTRA.E.EvidenceError, "separate checkout"):
                EXTRA.publish_staging(root, RUN, redactions, root)
            target = EXTRA.publish_staging(root, RUN, redactions, output_root)
            EXTRA.verify_staging(
                root, RUN, EXTRA.E.load_redactions(redactions), output_root
            )
            self.assertFalse((raw / "00-session.json").exists())
            receipt = json.loads((target / "publication.json").read_text())
            self.assertFalse(receipt["execution_authorized"])
            self.assertNotIn("123456789012", (target / "01-identity.txt").read_text())


if __name__ == "__main__":
    unittest.main()
