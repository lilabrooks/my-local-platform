from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock


SCRIPT = Path(__file__).parents[1] / "m4-preflight.py"
SPEC = importlib.util.spec_from_file_location("m4_preflight", SCRIPT)
assert SPEC and SPEC.loader
PREFLIGHT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PREFLIGHT)


class M4PreflightTest(unittest.TestCase):
    def test_rehearsals_require_clean_exact_candidate_and_actual_cleanup_fields(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            run_id = "20260910T180000Z"
            commit = "a" * 40
            raw = root / ".evidence" / "m4-local" / run_id
            raw.mkdir(parents=True)
            base = {
                "schema_version": 1,
                "result": "passed",
                "source_commit": commit,
                "worktree_clean": True,
            }
            values = {
                "k8s-sigterm.json": {
                    **base,
                    "cleanup": {
                        k: True
                        for k in (
                            "sink_baseline_restored",
                            "keda_pause_restored",
                            "port_forwards_stopped",
                            "database_lock_released",
                        )
                    },
                },
                "demo-rehearsal.json": {
                    **base,
                    "cleanup": {
                        k: True
                        for k in (
                            "keda_pause_absent",
                            "sink_baseline_restored",
                            "verification_port_forward_stopped",
                        )
                    },
                    "observations": {
                        "final_lag": 0,
                        "final_consumers": 1,
                        "peak_lag": 500,
                        "peak_consumers": 5,
                        "replay_completed": True,
                    },
                },
                "abort-rehearsal.json": {
                    **base,
                    "signal": "SIGTERM",
                    **{
                        k: True
                        for k in (
                            "resource_audit_empty",
                            "temporary_credentials_removed",
                            "temporary_workspace_removed",
                            "credential_canary_absent",
                        )
                    },
                },
                "capture-result.json": {
                    **base,
                    "run_id": run_id,
                    "environment": "local",
                    "cleanup_verified": True,
                    "provenance": {
                        role: [
                            {
                                "image_revision": commit,
                                "pod": role,
                                "pod_uid": "fixture-uid",
                                "image_id": "sha256:fixture",
                            }
                        ]
                        for role in ("relay-ingest", "relay-deliver", "sink")
                    },
                    "files": {
                        name: PREFLIGHT.hashlib.sha256(b"{}").hexdigest()
                        for name in PREFLIGHT.CAPTURE_FILES
                    },
                },
            }
            for filename in PREFLIGHT.CAPTURE_FILES:
                (raw / filename).write_text("{}")
            for filename, value in values.items():
                (raw / filename).write_text(json.dumps(value))
            self.assertEqual(
                len(PREFLIGHT.check_local_rehearsals(root, run_id, commit)["sha256"]), 4
            )
            artifact = raw / "10-event.json"
            artifact.write_text("changed")
            with self.assertRaisesRegex(PREFLIGHT.PreflightError, "artifact changed"):
                PREFLIGHT.check_local_rehearsals(root, run_id, commit)
            artifact.write_text("{}")
            path = raw / "k8s-sigterm.json"
            for change in (
                {"source_commit": "b" * 40},
                {"worktree_clean": False},
                {"result": "failed"},
                {"cleanup": {}},
            ):
                path.write_text(json.dumps({**values[path.name], **change}))
                with self.assertRaises(PREFLIGHT.PreflightError):
                    PREFLIGHT.check_local_rehearsals(root, run_id, commit)

    def terraform_fixture(self, root: Path) -> Path:
        directory = root / "infra" / "terraform" / "envs" / "dev"
        directory.mkdir(parents=True)
        blocks = "\n".join(
            f'variable "{flag}" {{\n  default = false\n}}'
            for flag in PREFLIGHT.HOURLY_FLAGS
        )
        (directory / "variables.tf").write_text(blocks + "\n", encoding="utf-8")
        (directory / "terraform.tfvars.example").write_text(
            "\n".join(f"{flag} = false" for flag in PREFLIGHT.HOURLY_FLAGS) + "\n",
            encoding="utf-8",
        )
        return directory

    def test_run_id_requires_a_real_canonical_utc_timestamp(self):
        PREFLIGHT.validate_run_id("20260907T210000Z")

        for value in ("", "2026-09-07", "20260230T210000Z", "20260907T210000"):
            with self.subTest(value=value), self.assertRaises(PREFLIGHT.PreflightError):
                PREFLIGHT.validate_run_id(value)

    def test_hcl_and_json_flags_fail_on_enabled_or_ambiguous_values(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            false_hcl = root / "false.tfvars"
            false_hcl.write_text("enable_eks = false\n", encoding="utf-8")
            self.assertEqual(PREFLIGHT.flag_values(false_hcl), {"enable_eks": False})

            true_json = root / "true.tfvars.json"
            true_json.write_text(json.dumps({"enable_msk": True}), encoding="utf-8")
            self.assertEqual(PREFLIGHT.flag_values(true_json), {"enable_msk": True})

            invalid = root / "invalid.tfvars"
            invalid.write_text("enable_rds = var.turn_it_on\n", encoding="utf-8")
            with self.assertRaisesRegex(PREFLIGHT.PreflightError, "non-boolean"):
                PREFLIGHT.flag_values(invalid)

    def test_terraform_arguments_find_enabled_flags_and_variable_files(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            variables = root / "runtime.tfvars"
            variables.write_text("enable_eks = true\n", encoding="utf-8")

            files, flags = PREFLIGHT.argument_variable_files(
                f"-var enable_msk=true -var-file={variables}", root
            )

            self.assertEqual(files, [variables])
            self.assertEqual(flags, {"enable_msk": True})

    def test_hourly_flags_fail_closed_across_active_configuration_sources(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            directory = self.terraform_fixture(root)

            PREFLIGHT.check_hourly_flags(root, {})

            active = directory / "terraform.tfvars"
            active.write_text("enable_eks = true\n", encoding="utf-8")
            with self.assertRaisesRegex(PREFLIGHT.PreflightError, "enable_eks"):
                PREFLIGHT.check_hourly_flags(root, {})

            active.unlink()
            active_json = directory / "terraform.tfvars.json"
            active_json.write_text(json.dumps({"enable_eks": True}), encoding="utf-8")
            with self.assertRaisesRegex(PREFLIGHT.PreflightError, "enable_eks"):
                PREFLIGHT.check_hourly_flags(root, {})

            active_json.unlink()
            with self.assertRaisesRegex(PREFLIGHT.PreflightError, "enable_msk"):
                PREFLIGHT.check_hourly_flags(root, {"TF_VAR_enable_msk": "true"})

            with self.assertRaisesRegex(PREFLIGHT.PreflightError, "enable_rds"):
                PREFLIGHT.check_hourly_flags(
                    root, {"AWS_TF_ARGS": "-var=enable_rds=true"}
                )

    def test_evidence_path_rejects_symlinks(self):
        if not hasattr(os, "symlink"):
            self.skipTest("symlinks are unavailable")
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            outside = root / "outside"
            outside.mkdir()
            (root / ".evidence").symlink_to(outside, target_is_directory=True)

            with self.assertRaisesRegex(PREFLIGHT.PreflightError, "symlink"):
                PREFLIGHT.ensure_no_symlink(
                    root / ".evidence" / "m4" / "20260907T210000Z", root
                )

    def test_rendered_documents_reject_local_endpoints_and_mutable_images(self):
        safe = {
            "application": {
                "images": ["example.invalid/relay=example.com/relay@sha256:" + "a" * 64]
            },
            "replay": {"image": "example.com/relay@sha256:" + "a" * 64},
        }
        PREFLIGHT.validate_rendered_documents(safe)

        cases = {
            "mutable image": {
                "application": {"images": ["example.invalid/relay=relay:latest"]},
                "replay": {"image": "example.com/relay@sha256:" + "a" * 64},
            },
            "local endpoint": {
                "application": safe["application"],
                "replay": {
                    "image": "example.com/relay@sha256:" + "a" * 64,
                    "endpoint": "host.minikube.internal:9094",
                },
            },
            "floating stable tag": {
                "application": {"images": ["example.invalid/relay=relay:stable"]},
                "replay": {"image": "example.com/relay@sha256:" + "a" * 64},
            },
        }
        for name, unsafe in cases.items():
            with self.subTest(name=name), self.assertRaises(PREFLIGHT.PreflightError):
                PREFLIGHT.validate_rendered_documents(unsafe)

    def test_missing_m3_receipt_fails(self):
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaisesRegex(PREFLIGHT.PreflightError, "is missing"):
                PREFLIGHT.check_m3_receipt(Path(temporary))

    def test_m3_receipt_rejects_a_non_object(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            receipt = root / PREFLIGHT.M3_RECEIPT.relative_to(PREFLIGHT.ROOT)
            receipt.parent.mkdir(parents=True)
            receipt.write_text("[]\n", encoding="utf-8")

            with self.assertRaisesRegex(PREFLIGHT.PreflightError, "JSON object"):
                PREFLIGHT.check_m3_receipt(root)

    def test_required_tool_check_names_every_missing_command(self):
        def installed(name: str):
            return None if name in {"aws", "minikube"} else f"/bin/{name}"

        with mock.patch.object(PREFLIGHT.shutil, "which", side_effect=installed):
            with self.assertRaisesRegex(PREFLIGHT.PreflightError, "aws, minikube"):
                PREFLIGHT.require_commands()

    def test_destroy_recipe_requires_identity_destroy_and_state_backup_in_order(self):
        valid = "\n".join(
            (
                "aws sts get-caller-identity",
                "test -f infra/terraform/envs/dev/.terraform/terraform.tfstate",
                "terraform destroy",
                "terraform state pull",
            )
        )
        PREFLIGHT.validate_destroy_recipe(valid)

        with self.assertRaises(PREFLIGHT.PreflightError):
            PREFLIGHT.validate_destroy_recipe(valid.replace("terraform destroy", ""))

    def test_image_revision_must_match_the_checked_commit(self):
        expected = "a" * 40
        labels = json.dumps({"org.opencontainers.image.revision": expected})
        image_id = "sha256:" + "c" * 64

        metadata = PREFLIGHT.validate_image_metadata(
            labels, image_id, "relay:dev", expected
        )

        self.assertEqual(metadata, {"id": image_id, "revision": expected})
        with self.assertRaisesRegex(PREFLIGHT.PreflightError, "expected"):
            PREFLIGHT.validate_image_metadata(labels, image_id, "relay:dev", "b" * 40)


if __name__ == "__main__":
    unittest.main()
