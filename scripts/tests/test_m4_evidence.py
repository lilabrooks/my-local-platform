from __future__ import annotations

import datetime
import hashlib
import importlib.util
import json
from pathlib import Path
import struct
import tempfile
import unittest
from unittest import mock
import zlib


SCRIPT = Path(__file__).parents[1] / "m4-evidence.py"
SPEC = importlib.util.spec_from_file_location("m4_evidence", SCRIPT)
assert SPEC and SPEC.loader
EVIDENCE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(EVIDENCE)

RUN_ID = "20260907T220000Z"
COMMIT = "a" * 40


class M4EvidenceTest(unittest.TestCase):
    def test_prefixed_duplicate_and_overdepth_json_cannot_hide_credentials(self):
        canary = 'synthetic quote" slash\\ secret'
        scan = {
            "entries": [
                {
                    "name": "DATABASE_PASSWORD",
                    "byte_length": len(canary.encode()),
                    "sha256": hashlib.sha256(canary.encode()).hexdigest(),
                }
            ]
        }
        nested = json.dumps({"message": json.dumps({"credential": canary})})
        prefixed = "[pod/relay-test/relay] " + nested
        duplicate = '{"message":' + json.dumps(nested) + ',"message":"safe"}'
        deep = nested
        for _ in range(6):
            deep = json.dumps({"message": deep})
        for value in (
            nested,
            prefixed,
            json.dumps({"logs": prefixed}),
            duplicate,
            deep,
        ):
            with self.subTest(value=value), self.assertRaises(EVIDENCE.EvidenceError):
                EVIDENCE.scan_secret_bytes(value.encode(), scan, "test")

    def test_failed_publication_works_with_partial_scan_without_diagnostics(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw = self.create_run(root)
            redactions = self.write_packet(raw)
            scan_path = raw / "secret-scan.json"
            scan = json.loads(scan_path.read_text())
            scan["state"] = "collecting"
            scan["entries"] = scan["entries"][:7]
            scan_path.write_text(json.dumps(scan))
            state = {
                "schema_version": 1,
                "run_id": RUN_ID,
                "commit": COMMIT,
                "phase": "complete",
                "cleanup_finished_at": "2026-09-08T00:00:00Z",
                "error": "synthetic-private-canary-SIGNING_SECRET",
                **{
                    key: 0
                    for key in (
                        "apply_exit",
                        "destroy_exit",
                        "terraform_state_exit",
                        "inventory_exit",
                        "cost_exit",
                    )
                },
                "cleanup_verified": True,
                "cleanup_overdue": False,
                "log_cleanup_passed": True,
                "transcript_passed": True,
            }
            (raw / "controller-state.json").write_text(json.dumps(state))
            destination = EVIDENCE.publish(root, RUN_ID, "failed", redactions)
            EVIDENCE.verify_failed(root, RUN_ID, EVIDENCE.load_redactions(redactions))
            summary = json.loads((destination / "failed-attempt.json").read_text())
            self.assertFalse(summary["demonstration_passed"])
            self.assertNotIn(
                "canary", (destination / "failed-attempt.json").read_text()
            )
            self.assertEqual(len(list(destination.iterdir())), 2)

    def test_scan_receipt_and_plaintext_credential_inputs_fail_closed(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw = self.create_run(root)
            redactions = self.write_packet(raw)
            path = raw / "secret-scan.json"
            original = path.read_text()
            for patch in (
                {"schema_version": True},
                {"credential": "not-allowed"},
                {"state": "collecting"},
                {"commit": "b" * 40},
            ):
                value = json.loads(original)
                value.update(patch)
                path.write_text(json.dumps(value))
                with self.assertRaises(EVIDENCE.EvidenceError):
                    EVIDENCE.load_secret_scan(raw, RUN_ID)
            path.write_text(original)
            (raw / "10-event.json").write_text(
                json.dumps({"encoded": "synthetic-private-canary-DATABASE_URL"})
            )
            with self.assertRaisesRegex(EVIDENCE.EvidenceError, "credential match"):
                EVIDENCE.publish(root, RUN_ID, "provisional", redactions)
            value = json.loads(redactions.read_text())
            value["values"]["SIGNING_SECRET"] = "placeholder"
            redactions.write_text(json.dumps(value))
            with self.assertRaisesRegex(EVIDENCE.EvidenceError, "plaintext credential"):
                EVIDENCE.load_redactions(redactions)

    def test_exclusive_write_and_orphan_scan_cannot_be_overwritten(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw = self.create_run(root)
            path = raw / "secret-scan.json"
            EVIDENCE.write_json_exclusive(path, {"orphan": True}, 0o600)
            original = path.read_bytes()
            with self.assertRaises(FileExistsError):
                EVIDENCE.write_json_exclusive(path, {"replaced": True}, 0o600)
            with self.assertRaisesRegex(EVIDENCE.EvidenceError, "already exists"):
                EVIDENCE.start_session(
                    root, RUN_ID, "2026-09-07T22:00:00Z", "owner", "us-east-1"
                )
            self.assertEqual(path.read_bytes(), original)
            self.assertFalse((raw / "00-session.json").exists())

    def create_run(self, root: Path) -> Path:
        raw = root / ".evidence" / "m4" / RUN_ID
        raw.mkdir(parents=True)
        (raw / "00-preflight.json").write_text(
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
        EVIDENCE.initialize_plan(root, RUN_ID)
        return raw

    def write_png(self, path: Path, width: int = 1280, height: int = 720) -> None:
        def chunk(kind: bytes, data: bytes) -> bytes:
            return (
                struct.pack(">I", len(data))
                + kind
                + data
                + struct.pack(">I", zlib.crc32(kind + data) & 0xFFFFFFFF)
            )

        scanline = b"\x00" + bytes((width + 7) // 8)
        path.write_bytes(
            EVIDENCE.PNG_SIGNATURE
            + chunk(
                b"IHDR", struct.pack(">II", width, height) + b"\x01\x00\x00\x00\x00"
            )
            + chunk(b"IDAT", zlib.compress(scanline * height))
            + chunk(b"IEND", b"")
        )

    def write_packet(self, raw: Path, *, include_final: bool = False) -> Path:
        filenames = EVIDENCE.FINAL_TEXT if include_final else EVIDENCE.PROVISIONAL_TEXT
        for name in filenames:
            path = raw / name
            if path.suffix == ".json":
                path.write_text(
                    json.dumps(
                        {
                            "account": "123456789012",
                            "registry": "123456789012.dkr.ecr.us-east-1.amazonaws.com",
                            "database": "db.abc.us-east-1.rds.amazonaws.com",
                            "broker": "boot-ab.c1.kafka-serverless.us-east-1.amazonaws.com",
                            "operator": "owner@example.com",
                            "address": "8.8.8.8",
                            "operator_note": "test operator note",
                        }
                    ),
                    encoding="utf-8",
                )
            else:
                path.write_text(
                    "account=123456789012 owner@example.com 8.8.8.8 "
                    "test operator note\n",
                    encoding="utf-8",
                )
        for name in EVIDENCE.REQUIRED_SCREENSHOTS:
            self.write_png(raw / name)
        (raw / "application-logs.txt").write_text("fixture logs")
        self.write_capture_receipts(raw)
        (raw / "visual-review.json").write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "run_id": RUN_ID,
                    "reviewed_at": "2026-09-07T22:30:00Z",
                    "reviewer": "local operator",
                    "result": "passed",
                    "files": list(EVIDENCE.REQUIRED_SCREENSHOTS),
                }
            ),
            encoding="utf-8",
        )
        redactions = raw / "redactions.json"
        redactions.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "values": {
                        "ACCOUNT_ID": "123456789012",
                        "OPERATOR_NOTE": "test operator note",
                        "OPERATOR": "local operator",
                    },
                }
            ),
            encoding="utf-8",
        )
        entries = []
        for name in EVIDENCE.SECRET_NAMES:
            value = ("synthetic-private-canary-" + name).encode()
            for representation in EVIDENCE.SECRET_REPRESENTATIONS:
                entries.append(
                    {
                        "name": name,
                        "representation": representation,
                        "byte_length": len(value),
                        "sha256": hashlib.sha256(value).hexdigest(),
                    }
                )
        EVIDENCE.write_json_exclusive(
            raw / "secret-scan.json",
            {
                "schema_version": 1,
                "run_id": RUN_ID,
                "commit": COMMIT,
                "session_sha256": EVIDENCE.hash_file(raw / "00-session.json"),
                "profile": "m4-secret-representations-v1",
                "state": "complete",
                "entries": entries,
            },
            0o600,
        )
        if include_final:
            (raw / "23-cost-final.txt").write_text(json.dumps(self.cost_receipt(raw)))
        return redactions

    def write_capture_receipts(self, raw):
        (raw / "00-session.json").write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "run_id": RUN_ID,
                    "commit": COMMIT,
                    "billable_started_at": "2026-09-07T22:00:00Z",
                }
            )
        )
        (raw / "01-identity.txt").write_text(
            json.dumps({"aws": {"account_id": "123456789012"}})
        )
        (raw / "capture-result.json").write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "run_id": RUN_ID,
                    "source_commit": COMMIT,
                    "result": "passed",
                    "environment": "aws",
                    "worktree_clean": True,
                    "started_at": "2026-09-07T22:00:00Z",
                    "finished_at": "2026-09-07T22:05:00Z",
                    "files": {
                        name: EVIDENCE.hash_file(raw / name)
                        for name in EVIDENCE.CAPTURE_FILES
                    },
                }
            )
        )
        (raw / "controller-state.json").write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "run_id": RUN_ID,
                    "commit": COMMIT,
                    "phase": "complete",
                    "result": "passed",
                    "cleanup_overdue": False,
                    "log_cleanup_passed": True,
                    "transcript_passed": True,
                    "apply_exit": 0,
                    "destroy_exit": 0,
                    "cost_exit": 0,
                    "terraform_state_exit": 0,
                    "inventory_exit": 0,
                    "cleanup_verified": True,
                    "cleanup_finished_at": "2026-09-07T22:30:00Z",
                }
            )
        )

    def cost_receipt(self, raw):
        query, _, _ = EVIDENCE.cost_context(raw, RUN_ID)
        first = EVIDENCE.datetime.date.fromisoformat(query["TimePeriod"]["Start"])
        last = EVIDENCE.datetime.date.fromisoformat(query["TimePeriod"]["End"])
        days = [
            {
                "TimePeriod": {
                    "Start": (first + EVIDENCE.datetime.timedelta(days=i)).isoformat(),
                    "End": (
                        first + EVIDENCE.datetime.timedelta(days=i + 1)
                    ).isoformat(),
                },
                "Estimated": False,
                "Groups": [
                    {
                        "Keys": ["Amazon EKS"],
                        "Metrics": {"UnblendedCost": {"Amount": "0.10", "Unit": "USD"}},
                    }
                ],
            }
            for i in range((last - first).days)
        ]
        return {
            "schema_version": 1,
            "run_id": RUN_ID,
            "source_commit": COMMIT,
            "session_sha256": EVIDENCE.hash_file(raw / "00-session.json"),
            "controller_sha256": EVIDENCE.hash_file(raw / "controller-state.json"),
            "collected_at": "2026-09-10T00:00:00Z",
            "attribution": "account-wide daily service totals; not exact M4 attribution",
            "tag_filter_used": False,
            "query": query,
            "pages": [{"GroupDefinitions": query["GroupBy"], "ResultsByTime": days}],
        }

    def test_final_cost_refuses_placeholder_early_estimated_and_partial_pages(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            raw = self.create_run(root)
            redactions = self.write_packet(raw, include_final=True)
            destination = EVIDENCE.publish(root, RUN_ID, "provisional", redactions)
            valid = self.cost_receipt(raw)
            variants = [
                {},
                {**valid, "collected_at": "2026-09-07T23:00:00Z"},
                {**valid, "tag_filter_used": True},
            ]
            estimated = json.loads(json.dumps(valid))
            estimated["pages"][0]["ResultsByTime"][0]["Estimated"] = True
            variants.append(estimated)
            partial = json.loads(json.dumps(valid))
            partial["pages"][0]["NextPageToken"] = "missing-next-page"
            variants.append(partial)
            for value in variants:
                (raw / "23-cost-final.txt").write_text(json.dumps(value))
                with self.assertRaises(EVIDENCE.EvidenceError):
                    EVIDENCE.publish(root, RUN_ID, "final", redactions)
                self.assertEqual(
                    json.loads((destination / "publication.json").read_text())["phase"],
                    "provisional",
                )
            (raw / "23-cost-final.txt").write_text("placeholder monthly account total")
            with self.assertRaises(EVIDENCE.EvidenceError):
                EVIDENCE.publish(root, RUN_ID, "final", redactions)

    def test_final_cost_month_boundary_and_pagination(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            raw = self.create_run(root)
            self.write_packet(raw)
            for name, key, value in (
                ("00-session.json", "billable_started_at", "2026-08-31T23:50:00Z"),
                (
                    "controller-state.json",
                    "cleanup_finished_at",
                    "2026-09-01T00:30:00Z",
                ),
            ):
                path = raw / name
                data = json.loads(path.read_text())
                data[key] = value
                path.write_text(json.dumps(data))
            receipt = self.cost_receipt(raw)
            self.assertEqual(
                receipt["query"]["TimePeriod"],
                {"Start": "2026-08-31", "End": "2026-09-02"},
            )
            page1 = receipt["pages"][0]
            page2 = json.loads(json.dumps(page1))
            page1["NextPageToken"] = "next"
            for day in page2["ResultsByTime"]:
                day["Groups"][0]["Keys"] = ["Amazon RDS"]
            with mock.patch.object(
                EVIDENCE,
                "private_aws_json",
                side_effect=[{"Account": "123456789012"}, page1, page2],
            ) as aws:
                path = EVIDENCE.collect_final_cost(root, RUN_ID)
            self.assertIn("--next-page-token", aws.call_args.args[1])
            self.assertEqual(len(json.loads(path.read_text())["pages"]), 2)
            EVIDENCE.validate_final_cost(raw, RUN_ID)

    def test_failed_snapshot_preserves_unknowns_and_survives_recovery(self):
        for phase, blocked in (
            ("cleanup_failed", "identity_unverified"),
            ("live", None),
        ):
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                raw = self.create_run(root)
                redactions = self.write_packet(raw)
                state = {
                    "schema_version": 1,
                    "run_id": RUN_ID,
                    "commit": COMMIT,
                    "phase": phase,
                    "apply_exit": -1,
                    "cleanup_blocked_reason": blocked,
                }
                (raw / "controller-state.json").write_text(json.dumps(state))
                destination = EVIDENCE.publish(root, RUN_ID, "failed", redactions)
                summary = json.loads((destination / "failed-attempt.json").read_text())
                self.assertFalse(summary["checks"]["cleanup_verified"])
                self.assertEqual(
                    summary["checks"]["destroy_exit"]["status"],
                    "not_run" if blocked else "unknown",
                )
                self.assertEqual(summary["checks"]["apply_exit"]["reported_exit"], -1)
                (raw / "controller-state.json").write_text(
                    json.dumps({**state, "phase": "complete"})
                )
                EVIDENCE.verify_failed(
                    root, RUN_ID, EVIDENCE.load_redactions(redactions)
                )

    def test_passing_publication_requires_capture_and_controller_proof(self):
        for target, change in (
            ("capture-result.json", None),
            ("capture-result.json", {"result": "failed"}),
            ("capture-result.json", {"environment": "local"}),
            ("controller-state.json", {"phase": "live"}),
            ("controller-state.json", {"cleanup_verified": False}),
            ("controller-state.json", {"result": "cleanup_overdue"}),
            ("controller-state.json", {"result": "cleanup_complete_with_errors"}),
            ("controller-state.json", {"cleanup_overdue": True}),
            ("controller-state.json", {"destroy_exit": 1}),
            ("controller-state.json", {"cost_exit": 1}),
            ("controller-state.json", {"log_cleanup_passed": False}),
            ("controller-state.json", {"transcript_passed": False}),
            ("10-event.json", {"changed": True}),
        ):
            with (
                self.subTest(target=target, change=change),
                tempfile.TemporaryDirectory() as tmp,
            ):
                root = Path(tmp)
                raw = self.create_run(root)
                redactions = self.write_packet(raw)
                path = raw / target
                if change is None:
                    path.unlink()
                else:
                    path.write_text(
                        json.dumps({**json.loads(path.read_text()), **change})
                    )
                with self.assertRaises(EVIDENCE.EvidenceError):
                    EVIDENCE.publish(root, RUN_ID, "provisional", redactions)

    def test_verification_rechecks_private_capture_receipt(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            raw = self.create_run(root)
            redactions = self.write_packet(raw)
            destination = EVIDENCE.publish(root, RUN_ID, "provisional", redactions)
            capture = raw / "capture-result.json"
            capture.write_text(capture.read_text() + "\n")
            with self.assertRaisesRegex(EVIDENCE.EvidenceError, "receipts changed"):
                EVIDENCE.verify_publication(
                    destination,
                    RUN_ID,
                    "provisional",
                    EVIDENCE.load_redactions(redactions),
                )

    def test_kafka_embedded_base64_canary_is_scanned(self):
        import base64

        secret = b"synthetic-secret-carrier-12345"
        scan = {
            "entries": [
                {
                    "name": "SIGNING_SECRET",
                    "byte_length": len(secret),
                    "sha256": hashlib.sha256(secret).hexdigest(),
                }
            ]
        }
        record = {
            "topic": "fixture",
            "partition": 0,
            "offset": 2,
            "value": base64.b64encode(b'{"secret":"' + secret + b'"}').decode(),
        }
        with self.assertRaisesRegex(EVIDENCE.EvidenceError, "credential match"):
            EVIDENCE.scan_secret_bytes(
                json.dumps(record).encode(), scan, "10-event.json"
            )

    def test_protocol_fixes_exports_visual_order_and_destroy_deadline(self):
        result = EVIDENCE.validate_protocol()
        captures = EVIDENCE.capture_steps()
        screenshots = EVIDENCE.screenshot_steps()

        self.assertEqual(result["final_file_count"], len(EVIDENCE.FINAL_TEXT))
        self.assertEqual(
            [item["file"] for item in screenshots if item.get("required")],
            list(EVIDENCE.REQUIRED_SCREENSHOTS),
        )
        self.assertEqual(
            screenshots[-1]["deadline_behavior"], "skip rather than delay destroy"
        )
        self.assertLess(
            next(
                i for i, item in enumerate(captures) if item["phase"] == "destroy_first"
            ),
            next(
                i for i, item in enumerate(captures) if item["phase"] == "after_destroy"
            ),
        )
        image_capture = next(
            item for item in captures if item["output"] == "05-images.json"
        )
        self.assertIn("make aws-stage-images", image_capture["command"])
        self.assertIn("AWS_APPROVED_COMMIT", image_capture["command"])
        plan_capture = next(
            item for item in captures if item["output"] == "03-plan-summary.json"
        )
        self.assertIn("make aws-plan", plan_capture["command"])
        account_capture = next(
            item for item in captures if item["output"] == "01-identity.txt"
        )
        self.assertIn("make aws-account-check", account_capture["command"])
        self.assertIn("AWS_APPROVED_COMMIT", account_capture["command"])
        self.assertIn("AWS_APPROVED_COMMIT", plan_capture["command"])
        release_capture = next(
            item for item in captures if item["output"] == "06-go-no-go.json"
        )
        self.assertIn("make aws-go-no-go", release_capture["command"])

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw = self.create_run(root)
            plan = json.loads((raw / "capture-plan.json").read_text(encoding="utf-8"))
            self.assertEqual(plan["failure_transition"]["next_phase"], "destroy_first")
            self.assertTrue(plan["failure_transition"]["skip_remaining_live_captures"])

    def test_start_session_sets_fixed_deadlines_from_billable_start(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.create_run(root)

            path = EVIDENCE.start_session(
                root,
                RUN_ID,
                "2026-09-07T22:00:00Z",
                "operator-one",
                "us-east-1",
            )
            session = json.loads(path.read_text(encoding="utf-8"))

            self.assertEqual(session["destroy_deadline"], "2026-09-08T00:30:00Z")
            self.assertEqual(session["hard_deadline"], "2026-09-08T01:00:00Z")
            self.assertEqual(session["commit"], COMMIT)

    def test_live_controller_permit_requires_fresh_matching_process(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw = self.create_run(root)
            EVIDENCE.start_session(
                root,
                RUN_ID,
                "2026-09-07T22:00:00Z",
                "operator-one",
                "us-east-1",
            )
            (raw / "controller-state.json").write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "run_id": RUN_ID,
                        "commit": COMMIT,
                        "region": "us-east-1",
                        "controller_pid": 1234,
                        "phase": "applying",
                        "updated_at": "2026-09-07T22:15:00Z",
                    }
                ),
                encoding="utf-8",
            )

            result = EVIDENCE.validate_live_controller(
                root,
                RUN_ID,
                COMMIT,
                1234,
                now=datetime.datetime(2026, 9, 7, 22, 15, tzinfo=datetime.UTC),
                pid_check=lambda pid: pid == 1234,
            )

            self.assertEqual(result["controller_pid"], 1234)
            self.assertEqual(result["destroy_deadline"], "2026-09-08T00:30:00Z")

    def test_live_controller_permit_rejects_stale_or_dead_process(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw = self.create_run(root)
            EVIDENCE.start_session(
                root,
                RUN_ID,
                "2026-09-07T22:00:00Z",
                "operator-one",
                "us-east-1",
            )
            (raw / "controller-state.json").write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "run_id": RUN_ID,
                        "commit": COMMIT,
                        "region": "us-east-1",
                        "controller_pid": 1234,
                        "phase": "applying",
                        "updated_at": "2026-09-07T22:01:00Z",
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(EVIDENCE.EvidenceError, "too old"):
                EVIDENCE.validate_live_controller(
                    root,
                    RUN_ID,
                    COMMIT,
                    1234,
                    now=datetime.datetime(2026, 9, 7, 22, 16, tzinfo=datetime.UTC),
                    pid_check=lambda _pid: True,
                )
            with self.assertRaisesRegex(EVIDENCE.EvidenceError, "heartbeat is stale"):
                EVIDENCE.validate_live_controller(
                    root,
                    RUN_ID,
                    COMMIT,
                    1234,
                    now=datetime.datetime(2026, 9, 7, 22, 1, 6, tzinfo=datetime.UTC),
                    pid_check=lambda _pid: True,
                )
            with self.assertRaisesRegex(EVIDENCE.EvidenceError, "not running"):
                EVIDENCE.validate_live_controller(
                    root,
                    RUN_ID,
                    COMMIT,
                    1234,
                    now=datetime.datetime(2026, 9, 7, 22, 1, tzinfo=datetime.UTC),
                    pid_check=lambda _pid: False,
                )

    def test_publication_redacts_text_and_requires_reviewed_visuals(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw = self.create_run(root)
            redactions = self.write_packet(raw)

            destination = EVIDENCE.publish(root, RUN_ID, "provisional", redactions)
            receipt = EVIDENCE.verify_publication(
                destination,
                RUN_ID,
                "provisional",
                EVIDENCE.load_redactions(redactions),
            )
            event = (destination / "10-event.json").read_text(encoding="utf-8")

            self.assertEqual(receipt["phase"], "provisional")
            self.assertIn("[REDACTED:ACCOUNT_ID]", event)
            self.assertIn("[REDACTED:ECR_REGISTRY]", event)
            self.assertIn("[REDACTED:RDS_ENDPOINT]", event)
            self.assertIn("[REDACTED:MSK_ENDPOINT]", event)
            self.assertIn("[REDACTED:EMAIL]", event)
            self.assertIn("[REDACTED:PUBLIC_IP]", event)
            self.assertIn("[REDACTED:OPERATOR_NOTE]", event)

    def test_sanitizer_preserves_commit_digests_timestamps_and_private_addresses(self):
        redactions = {
            "ACCOUNT_ID": "123456789012",
            "DATABASE_PASSWORD": "database-password",
            "OPERATOR": "operator-one",
            "SIGNING_SECRET": "signing-secret",
        }
        digest = "sha256:" + "123456789012" + "a" * 52
        source = (
            f"commit={COMMIT} digest={digest} time=2026-09-07T22:30:00Z "
            "private=10.0.0.12 account=123456789012"
        )

        sanitized = EVIDENCE.sanitize_text(source, redactions)

        self.assertIn(COMMIT, sanitized)
        self.assertIn(digest, sanitized)
        self.assertIn("2026-09-07T22:30:00Z", sanitized)
        self.assertIn("10.0.0.12", sanitized)
        self.assertIn("[REDACTED:ACCOUNT_ID]", sanitized)

    def test_hex_shaped_secret_is_redacted_and_detected_if_it_survives(self):
        hex_alphabet = "0123456789abcdef"
        secret = hex_alphabet * 2 + hex_alphabet[:8] + "=="
        redactions = {
            "ACCOUNT_ID": "123456789012",
            "DATABASE_PASSWORD": "database-password",
            "OPERATOR": "operator-one",
            "SIGNING_SECRET": secret,
        }
        source = f"RELAY_SIGNING_SECRET={secret}\n"

        sanitized = EVIDENCE.sanitize_text(source, redactions)

        self.assertNotIn(secret, sanitized)
        self.assertIn("[REDACTED:SIGNING_SECRET]", sanitized)
        self.assertEqual(EVIDENCE.sensitive_matches(sanitized, redactions), [])
        self.assertEqual(
            EVIDENCE.sensitive_matches(source, redactions), ["SIGNING_SECRET"]
        )

    def test_publication_blocks_an_unreviewed_screenshot(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw = self.create_run(root)
            redactions = self.write_packet(raw)
            review_path = raw / "visual-review.json"
            review = json.loads(review_path.read_text(encoding="utf-8"))
            review["files"].remove("tempo-trace.png")
            review_path.write_text(json.dumps(review), encoding="utf-8")

            with self.assertRaisesRegex(EVIDENCE.EvidenceError, "every required"):
                EVIDENCE.publish(root, RUN_ID, "provisional", redactions)

    def test_visual_review_requires_a_reviewer_and_real_utc_time(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw = self.create_run(root)
            self.write_packet(raw)
            review_path = raw / "visual-review.json"
            review = json.loads(review_path.read_text(encoding="utf-8"))
            review["reviewer"] = ""
            review_path.write_text(json.dumps(review), encoding="utf-8")

            with self.assertRaisesRegex(EVIDENCE.EvidenceError, "every required"):
                EVIDENCE.validate_visual_review(raw, RUN_ID)

            review["reviewer"] = "local operator"
            review["reviewed_at"] = "2026-02-30T22:30:00Z"
            review_path.write_text(json.dumps(review), encoding="utf-8")
            with self.assertRaisesRegex(EVIDENCE.EvidenceError, "real UTC"):
                EVIDENCE.validate_visual_review(raw, RUN_ID)

    def test_publication_blocks_a_small_or_invalid_screenshot(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw = self.create_run(root)
            redactions = self.write_packet(raw)
            self.write_png(raw / "grafana-lag.png", 320, 200)

            with self.assertRaisesRegex(EVIDENCE.EvidenceError, "too small"):
                EVIDENCE.publish(root, RUN_ID, "provisional", redactions)

            self.write_png(raw / "grafana-lag.png")
            content = (raw / "grafana-lag.png").read_bytes()
            (raw / "grafana-lag.png").write_bytes(content[:33])
            with self.assertRaisesRegex(EVIDENCE.EvidenceError, "complete PNG"):
                EVIDENCE.publish(root, RUN_ID, "provisional", redactions)

    def test_verifier_detects_post_publication_edits(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw = self.create_run(root)
            redactions = self.write_packet(raw)
            destination = EVIDENCE.publish(root, RUN_ID, "provisional", redactions)
            (destination / "12-metrics.txt").write_text("changed\n", encoding="utf-8")

            with self.assertRaisesRegex(EVIDENCE.EvidenceError, "hash changed"):
                EVIDENCE.verify_publication(
                    destination,
                    RUN_ID,
                    "provisional",
                    EVIDENCE.load_redactions(redactions),
                )

    def test_verifier_rejects_an_unlisted_directory(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw = self.create_run(root)
            redactions = self.write_packet(raw)
            destination = EVIDENCE.publish(root, RUN_ID, "provisional", redactions)
            (destination / "extra").mkdir()

            with self.assertRaisesRegex(EVIDENCE.EvidenceError, "unexpected"):
                EVIDENCE.verify_publication(
                    destination,
                    RUN_ID,
                    "provisional",
                    EVIDENCE.load_redactions(redactions),
                )

    def test_final_publication_adds_settled_cost_to_a_verified_packet(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw = self.create_run(root)
            redactions = self.write_packet(raw, include_final=True)
            destination = EVIDENCE.publish(root, RUN_ID, "provisional", redactions)

            EVIDENCE.publish(root, RUN_ID, "final", redactions)
            receipt = EVIDENCE.verify_publication(
                destination,
                RUN_ID,
                "final",
                EVIDENCE.load_redactions(redactions),
            )

            self.assertEqual(receipt["phase"], "final")
            self.assertTrue((destination / "23-cost-final.txt").is_file())


if __name__ == "__main__":
    unittest.main()
