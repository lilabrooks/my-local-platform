from __future__ import annotations

import datetime
import importlib.util
import json
from pathlib import Path
import struct
import tempfile
import unittest
import zlib


SCRIPT = Path(__file__).parents[1] / "m4-evidence.py"
SPEC = importlib.util.spec_from_file_location("m4_evidence", SCRIPT)
assert SPEC and SPEC.loader
EVIDENCE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(EVIDENCE)

RUN_ID = "20260907T220000Z"
COMMIT = "a" * 40


class M4EvidenceTest(unittest.TestCase):
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
                            "secret": "correct horse battery staple",
                        }
                    ),
                    encoding="utf-8",
                )
            else:
                path.write_text(
                    "account=123456789012 owner@example.com 8.8.8.8 "
                    "correct horse battery staple\n",
                    encoding="utf-8",
                )
        for name in EVIDENCE.REQUIRED_SCREENSHOTS:
            self.write_png(raw / name)
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
                        "DATABASE_PASSWORD": "database-password",
                        "SIGNING_SECRET": "correct horse battery staple",
                        "OPERATOR": "local operator",
                    },
                }
            ),
            encoding="utf-8",
        )
        return redactions

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
                    now=datetime.datetime(
                        2026, 9, 7, 22, 1, 6, tzinfo=datetime.UTC
                    ),
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
            self.assertIn("[REDACTED:SIGNING_SECRET]", event)

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
