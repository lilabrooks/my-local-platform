from __future__ import annotations

import importlib.util
import base64
from concurrent.futures import Future
import json
import io
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest import mock

SCRIPT = Path(__file__).parents[1] / "m4-live-capture.py"
SPEC = importlib.util.spec_from_file_location("m4_capture", SCRIPT)
CAPTURE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CAPTURE)
EVIDENCE = CAPTURE.load_script("m4-evidence")
COMMIT = "a" * 40
RUN = "20260910T180000Z"


class CaptureTests(unittest.TestCase):
    def args(self):
        return SimpleNamespace(
            environment="local",
            context="mlp",
            run_id=RUN,
            commit=COMMIT,
            image="relay:dev",
            deploy=False,
        )

    def test_failure_stops_before_proof_and_receipt_is_not_a_pass(self):
        with (
            tempfile.TemporaryDirectory() as temporary,
            mock.patch.object(CAPTURE, "ROOT", Path(temporary)),
            mock.patch.object(CAPTURE, "load_script", return_value=EVIDENCE),
        ):
            capture = CAPTURE.Capture(self.args())
            capture.setup = mock.Mock(side_effect=CAPTURE.CaptureError("setup failed"))
            capture.proof = mock.Mock()
            capture.stop = mock.Mock()
            with self.assertRaises(CAPTURE.CaptureError):
                capture.run()
            capture.stop.assert_called_once()
            capture.proof.assert_not_called()
            receipt = json.loads((capture.raw / "capture-result.json").read_text())
            self.assertEqual(receipt["result"], "failed")
            self.assertFalse(receipt["worktree_clean"])
            with self.assertRaises(FileExistsError):
                capture.run()
            self.assertEqual(capture.setup.call_count, 1)

    def test_expired_capture_stops_before_starting_a_command(self):
        capture = CAPTURE.Capture(self.args())
        capture.deadline = 0
        with mock.patch.object(CAPTURE.subprocess, "Popen") as process:
            with self.assertRaisesRegex(CAPTURE.CaptureError, "deadline"):
                capture.command(["kubectl", "apply"])
            process.assert_not_called()

    def test_cleanup_reads_back_and_attempts_both_restorations(self):
        capture = CAPTURE.Capture(self.args())
        capture.changed_sink = capture.changed_pause = True
        capture.http = mock.Mock(
            side_effect=[
                CAPTURE.CaptureError("write failed"),
                {"latency_ms": 0, "fail_rate": 0},
            ]
        )
        capture.kubectl = mock.Mock()
        capture.get = mock.Mock(return_value={"metadata": {}})
        self.assertFalse(capture.cleanup_local())
        capture.kubectl.assert_called_once()
        capture.http = mock.Mock(return_value={"latency_ms": 1000, "fail_rate": 0})
        self.assertFalse(capture.cleanup_local())
        capture.http.return_value = {"latency_ms": 0, "fail_rate": 0}
        self.assertTrue(capture.cleanup_local())
        capture.get.return_value = {"metadata": {"annotations": {CAPTURE.PAUSE: "0"}}}
        self.assertFalse(capture.cleanup_local())

    def test_rejected_stop_is_reported_and_not_recorded_as_accepted(self):
        capture = CAPTURE.Capture(self.args())
        capture.args.environment = "aws"
        with (
            mock.patch.object(
                CAPTURE.subprocess,
                "run",
                side_effect=CAPTURE.subprocess.CalledProcessError(2, "make"),
            ),
            mock.patch.object(
                CAPTURE.sys, "stderr", new_callable=io.StringIO
            ) as stderr,
        ):
            capture.stop()
        self.assertTrue(capture.stop_attempted)
        self.assertFalse(capture.stop_requested)
        self.assertIn("Stop request failed", stderr.getvalue())

    def test_initialization_failure_still_requests_aws_cleanup(self):
        with (
            mock.patch.object(
                CAPTURE.sys,
                "argv",
                [
                    "capture",
                    "--environment",
                    "aws",
                    "--context",
                    "expected",
                    "--run-id",
                    RUN,
                    "--commit",
                    COMMIT,
                ],
            ),
            mock.patch.object(
                CAPTURE, "Capture", side_effect=CAPTURE.CaptureError("invalid GO")
            ),
            mock.patch.object(CAPTURE, "request_stop") as stop,
            mock.patch.object(CAPTURE.signal, "signal"),
            mock.patch.object(CAPTURE.sys, "stderr", new_callable=io.StringIO),
        ):
            self.assertEqual(CAPTURE.main(), 1)
        stop.assert_called_once_with(RUN)

    def test_aws_capture_pass_does_not_claim_resource_cleanup(self):
        with (
            tempfile.TemporaryDirectory() as temporary,
            mock.patch.object(CAPTURE, "ROOT", Path(temporary)),
            mock.patch.object(CAPTURE, "load_script", return_value=EVIDENCE),
        ):
            capture = CAPTURE.Capture(self.args())
            capture.args.environment = "aws"
            capture.setup = mock.Mock()
            capture.proof = mock.Mock()
            capture.set_phase_deadline = mock.Mock()
            capture.stop = mock.Mock()
            capture.run()
            receipt = json.loads((capture.raw / "capture-result.json").read_text())
            self.assertEqual(receipt["result"], "passed")
            self.assertFalse(receipt["cleanup_verified"])

    def test_missing_workload_and_initial_scrape_are_waited_for(self):
        capture = CAPTURE.Capture(self.args())
        capture.guard = mock.Mock()
        capture.kubectl = mock.Mock(side_effect=[b"", b'{"metadata":{"name":"relay"}}'])
        with mock.patch.object(CAPTURE.time, "sleep"):
            self.assertEqual(
                capture.wait_for_resource("mlp", "deployment/relay")["metadata"][
                    "name"
                ],
                "relay",
            )
        self.assertIn("--ignore-not-found", capture.kubectl.call_args.args)
        capture.sample = mock.Mock(
            side_effect=[
                CAPTURE.ObservationPending("empty"),
                {"lag": 0, "replicas": 1, "members": 1},
            ]
        )
        with mock.patch.object(CAPTURE.time, "sleep"):
            self.assertEqual(capture.baseline()["members"], 1)

    def test_sampling_error_cancels_queue_before_pool_drains(self):
        for failure in (CAPTURE.CaptureError("sampling failed"), KeyboardInterrupt()):
            capture = CAPTURE.Capture(self.args())
            started = []
            eight_started = threading.Event()
            released = threading.Event()

            def worker(
                i,
                capture=capture,
                started=started,
                eight_started=eight_started,
                released=released,
            ):
                capture.guard()
                started.append(i)
                if len(started) == 8:
                    eight_started.set()
                released.wait(timeout=2)

            def stopped(capture=capture, released=released):
                self.assertTrue(capture.cancelled.is_set())
                self.assertFalse(released.is_set())
                released.set()

            capture.stop = mock.Mock(side_effect=stopped)
            with self.assertRaises(type(failure)):
                with capture.load_pool() as pool:
                    futures = [pool.submit(worker, i) for i in range(600)]
                    self.assertTrue(eight_started.wait(timeout=2))
                    raise failure
            self.assertEqual(len(started), 8)
            capture.stop.assert_called_once()
            self.assertTrue(all(f.done() for f in futures))

    def test_deploy_budget_reserves_proof_and_never_extends_destroy(self):
        capture = CAPTURE.Capture(self.args())
        capture.args.environment = "aws"
        now = CAPTURE.dt.datetime.now(CAPTURE.dt.UTC)
        session = {
            "destroy_deadline": (now + CAPTURE.dt.timedelta(minutes=90)).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
        }
        with (
            mock.patch.object(
                capture.evidence, "read_json_object", return_value=session
            ),
            mock.patch.object(CAPTURE.time, "monotonic", return_value=100),
        ):
            capture.set_phase_deadline(deployment=True)
            self.assertAlmostEqual(capture.deadline, 100 + 70 * 60, delta=2)
            capture.set_phase_deadline()
            self.assertEqual(capture.deadline, 100 + CAPTURE.PROOF_SECONDS)
            session["destroy_deadline"] = (
                now + CAPTURE.dt.timedelta(minutes=10)
            ).strftime("%Y-%m-%dT%H:%M:%SZ")
            with self.assertRaisesRegex(CAPTURE.CaptureError, "reserve"):
                capture.set_phase_deadline(deployment=True)

    def test_stop_defers_signals_and_uses_separate_process_group(self):
        def run(*args, **kwargs):
            self.assertTrue(kwargs["start_new_session"])
            self.assertEqual(
                CAPTURE.signal.getsignal(CAPTURE.signal.SIGINT), CAPTURE.signal.SIG_IGN
            )

        original = CAPTURE.signal.getsignal(CAPTURE.signal.SIGINT)
        with (
            mock.patch.object(CAPTURE.subprocess, "run", side_effect=run),
            mock.patch.object(CAPTURE.sys, "stderr", new_callable=io.StringIO),
        ):
            self.assertTrue(CAPTURE.request_stop(RUN))
        self.assertEqual(CAPTURE.signal.getsignal(CAPTURE.signal.SIGINT), original)

    def test_failed_read_only_verification_does_not_stop_controller(self):
        with (
            mock.patch.object(
                CAPTURE.sys,
                "argv",
                [
                    "capture",
                    "--environment",
                    "aws",
                    "--context",
                    "expected",
                    "--run-id",
                    RUN,
                    "--commit",
                    COMMIT,
                    "--verify-output",
                    "10-event.json",
                ],
            ),
            mock.patch.object(
                CAPTURE, "Capture", side_effect=CAPTURE.CaptureError("bad receipt")
            ),
            mock.patch.object(CAPTURE, "request_stop") as stop,
            mock.patch.object(CAPTURE.signal, "signal"),
            mock.patch.object(CAPTURE.sys, "stderr", new_callable=io.StringIO),
        ):
            self.assertEqual(CAPTURE.main(), 1)
        stop.assert_not_called()

    def test_stale_member_sample_cannot_prove_scaleout(self):
        capture = CAPTURE.Capture(self.args())
        capture.metric = mock.Mock(
            side_effect=lambda q: 31 if "timestamp(relay_group_members" in q else 0
        )
        with self.assertRaisesRegex(CAPTURE.ObservationPending, "metric sample"):
            capture.sample()

    def test_replica_scrape_has_its_own_age_margin(self):
        for age, accepted in ((31, True), (60, True), (61, False)):
            with self.subTest(age=age):
                capture = CAPTURE.Capture(self.args())
                capture.metric = mock.Mock(
                    side_effect=lambda q, age=age: (
                        age if "timestamp(kube_deployment_spec_replicas" in q else 0
                    )
                )
                if accepted:
                    self.assertEqual(capture.sample()["replicas"], 0)
                else:
                    with self.assertRaises(CAPTURE.ObservationPending):
                        capture.sample()

    def test_drain_retries_only_pending_samples_within_original_deadline(self):
        for persistent in (False, True):
            with self.subTest(persistent=persistent):
                capture = CAPTURE.Capture(self.args())
                clock = [0]
                capture.deadline = 1200
                pending = CAPTURE.ObservationPending("stale")
                fresh = {"lag": 0, "replicas": 1, "members": 1}
                capture.sample = mock.Mock(
                    side_effect=pending if persistent else [pending, fresh]
                )
                with (
                    mock.patch.object(
                        CAPTURE.time,
                        "monotonic",
                        side_effect=lambda clock=clock: clock[0],
                    ),
                    mock.patch.object(
                        CAPTURE.time,
                        "sleep",
                        side_effect=lambda seconds, clock=clock: clock.__setitem__(
                            0, clock[0] + seconds
                        ),
                    ),
                ):
                    if persistent:
                        with self.assertRaisesRegex(CAPTURE.CaptureError, "deadline"):
                            capture.wait(capture.drained_sample, 120)
                        self.assertEqual(clock[0], 120)
                        self.assertEqual(capture.sample.call_count, 120)
                    else:
                        self.assertEqual(
                            capture.wait(capture.drained_sample, 120), fresh
                        )
                        self.assertEqual(clock[0], 1)
                capture.sample.side_effect = CAPTURE.CaptureError("real failure")
                with self.assertRaisesRegex(CAPTURE.CaptureError, "real failure"):
                    capture.drained_sample()

    def test_drain_cannot_accept_a_probe_that_finishes_at_or_after_deadline(self):
        for finished_at in (120, 121):
            with self.subTest(finished_at=finished_at):
                capture = CAPTURE.Capture(self.args())
                capture.deadline = 1200
                clock = [0]

                def late_sample(clock=clock, finished_at=finished_at):
                    clock[0] = finished_at
                    return {"lag": 0, "replicas": 1, "members": 1}

                capture.sample = mock.Mock(side_effect=late_sample)
                with mock.patch.object(
                    CAPTURE.time, "monotonic", side_effect=lambda clock=clock: clock[0]
                ):
                    with self.assertRaisesRegex(CAPTURE.CaptureError, "deadline"):
                        capture.wait(capture.drained_sample, 120)
                capture.sample.assert_called_once()

    def test_wait_rechecks_cancellation_after_a_successful_probe(self):
        capture = CAPTURE.Capture(self.args())

        def probe():
            capture.cancelled.set()
            return True

        with self.assertRaisesRegex(CAPTURE.CaptureError, "stopped"):
            capture.wait(probe, 120)

    def test_real_proof_load_retries_pending_but_times_out_or_stops_on_hard_error(self):
        class ReachedDLQ(Exception):
            pass

        class ImmediatePool:
            # Deterministic producer scheduling; the existing cancellation test
            # separately exercises eight real threads and 592 queued requests.
            def __init__(self, **kwargs):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

            def submit(self, fn, *args):
                future = Future()
                future.set_result(fn(*args))
                return future

            def shutdown(self, **kwargs):
                pass

        for mode in ("transient", "persistent", "hard"):
            with self.subTest(mode=mode):
                capture = CAPTURE.Capture(self.args())
                capture.deadline = 1200
                clock = [0]
                event_id = "evt_" + "a" * 32
                observed = {"posted": [], "controls": [], "request": None, "samples": 0}
                fresh = {"lag": 0, "replicas": 1, "members": 1}
                capture.baseline = mock.Mock(return_value=fresh)
                capture.get = mock.Mock(return_value={"metadata": {}})
                capture.write = mock.Mock()
                capture.stop = mock.Mock()

                def http(
                    service,
                    path,
                    body=None,
                    *,
                    observed=observed,
                    event_id=event_id,
                    **kwargs,
                ):
                    if path == "/control":
                        if body is not None:
                            observed["controls"].append(
                                (dict(body), observed["samples"])
                            )
                        return {"latency_ms": 0, "fail_rate": 0}
                    if path == "/v1/events":
                        if body["type"] == "m4.load":
                            observed["posted"].append(body)
                            return {"id": str(body["data"]["sequence"])}
                        observed["request"] = body
                        return {"id": event_id}
                    if path.endswith("/attempts"):
                        return {
                            "attempts": [
                                {"outcome": "delivered"},
                                {"outcome": "exhausted"},
                            ]
                        }
                    if path == "/received":
                        return {
                            "deliveries": [
                                {
                                    "webhook_id": event_id,
                                    "path": "/hooks/ok",
                                    "status": 200,
                                    "data": observed["request"]["data"],
                                    "type": "m4.proof",
                                }
                            ]
                        }
                    if service == "tempo":
                        return {"trace": "fixture"}
                    raise AssertionError((service, path))

                def job(operation, *, event_id=event_id, **kwargs):
                    if operation == "poison":
                        raise ReachedDLQ
                    if operation == "read":
                        return {
                            "records": [
                                {
                                    "value": base64.b64encode(
                                        json.dumps({"id": event_id}).encode()
                                    ).decode()
                                }
                            ]
                        }
                    return {}

                def sample(observed=observed, mode=mode, fresh=fresh):
                    observed["samples"] += 1
                    if observed["samples"] == 1:
                        return {"lag": 500, "replicas": 12, "members": 12}
                    if mode == "hard":
                        raise CAPTURE.CaptureError("hard sample failure")
                    if mode == "persistent" or observed["samples"] == 2:
                        raise CAPTURE.ObservationPending("stale replicas")
                    return fresh

                capture.http = mock.Mock(side_effect=http)
                capture.job = mock.Mock(side_effect=job)
                capture.sample = mock.Mock(side_effect=sample)
                with (
                    mock.patch.object(CAPTURE, "ThreadPoolExecutor", ImmediatePool),
                    mock.patch.object(CAPTURE, "assert_attempts", return_value=[]),
                    mock.patch.object(CAPTURE, "assert_trace"),
                    mock.patch.object(
                        CAPTURE.time,
                        "monotonic",
                        side_effect=lambda clock=clock: clock[0],
                    ),
                    mock.patch.object(
                        CAPTURE.time,
                        "sleep",
                        side_effect=lambda seconds, clock=clock: clock.__setitem__(
                            0, clock[0] + seconds
                        ),
                    ),
                    mock.patch("builtins.print"),
                ):
                    with self.assertRaises(
                        ReachedDLQ if mode == "transient" else CAPTURE.CaptureError
                    ):
                        capture.proof()
                self.assertEqual(len(observed["posted"]), 600)
                self.assertEqual(len({p["tenant_id"] for p in observed["posted"]}), 16)
                outputs = {
                    call.args[0]: call.args[1] for call in capture.write.call_args_list
                }
                if mode == "transient":
                    self.assertEqual(observed["samples"], 3)
                    self.assertEqual(
                        observed["controls"][-1], ({"latency_ms": 0, "fail_rate": 0}, 3)
                    )
                    self.assertEqual(
                        len(outputs["12-metrics.txt"]["samples"]), 3
                    )  # baseline plus two fresh
                    self.assertEqual(
                        len(set(outputs["12-metrics.txt"]["load_event_ids"])), 600
                    )
                    self.assertFalse(capture.cancelled.is_set())
                    capture.stop.assert_not_called()
                else:
                    self.assertTrue(capture.cancelled.is_set())
                    capture.stop.assert_called_once()
                    self.assertNotIn("12-metrics.txt", outputs)
                    self.assertEqual(len(observed["controls"]), 1)  # delay held
                    if mode == "persistent":
                        self.assertEqual(clock[0], 480)

    def test_fake_aws_deploy_waits_for_generated_objects_then_initial_scrape(self):
        with tempfile.TemporaryDirectory() as tmp:
            capture = CAPTURE.Capture(self.args())
            capture.raw = Path(tmp)
            capture.args.environment = "aws"
            capture.args.deploy = True
            capture.environment["KUBECONFIG"] = str(Path(tmp) / "kubeconfig")
            packet = {
                "aws_profile": "fixture",
                "image_references": {
                    "relay": "relay@sha256:" + "a" * 64,
                    "sink": "sink@sha256:" + "b" * 64,
                },
            }
            (capture.raw / "06-go-no-go.json").write_text(json.dumps(packet))
            capture.guard = mock.Mock()
            capture.set_phase_deadline = mock.Mock()
            capture.require_current_context = mock.Mock()
            capture.command = mock.Mock(
                side_effect=lambda args, **kwargs: (
                    COMMIT.encode() if args[:2] == ["git", "rev-parse"] else b""
                )
            )
            capture.aws_command = mock.Mock(return_value=b"fixture-brokers")
            seen = {}

            def kubectl(*args, **kwargs):
                if "--ignore-not-found" in args:
                    resource = args[3]
                    seen[resource] = seen.get(resource, 0) + 1
                    return b"" if seen[resource] == 1 else b'{"metadata":{}}'
                if "rollout" in args:
                    self.assertEqual(seen[args[4]], 2)
                if "get" in args:
                    service = "sink" if args[3] == "deployment/sink" else "relay"
                    return json.dumps(
                        {
                            "spec": {
                                "template": {
                                    "spec": {
                                        "containers": [
                                            {
                                                "image": packet["image_references"][
                                                    service
                                                ]
                                            }
                                        ]
                                    }
                                }
                            }
                        }
                    ).encode()
                return b""

            capture.kubectl = mock.Mock(side_effect=kubectl)
            capture.forward = mock.Mock(
                side_effect=lambda service, ns, resource, port: self.assertEqual(
                    seen[resource], 2
                )
            )
            with (
                mock.patch.object(
                    capture.evidence, "load_secret_scan", return_value={}
                ),
                mock.patch.object(CAPTURE.time, "sleep"),
            ):
                capture.setup()
                capture.sample = mock.Mock(
                    side_effect=[
                        CAPTURE.ObservationPending("no scrape"),
                        {"lag": 0, "replicas": 1, "members": 1},
                    ]
                )
                self.assertEqual(capture.baseline()["members"], 1)
            self.assertEqual(capture.forward.call_count, 4)
            installs = [
                call.args[0][1]
                for call in capture.command.call_args_list
                if call.args[0][0] == "make"
            ]
            self.assertEqual(
                installs,
                ["aws-kubeconfig", "aws-k8s-render", *CAPTURE.INSTALL_TIMEOUTS],
            )

    def test_duplicate_aws_capture_does_not_request_stop(self):
        with tempfile.TemporaryDirectory() as tmp:
            capture = CAPTURE.Capture(self.args())
            capture.raw = Path(tmp)
            capture.args.environment = "aws"
            capture.write("capture-start.json", {"already": "claimed"})
            with (
                mock.patch.object(
                    CAPTURE.sys,
                    "argv",
                    [
                        "capture",
                        "--environment",
                        "aws",
                        "--context",
                        "mlp",
                        "--run-id",
                        RUN,
                        "--commit",
                        COMMIT,
                    ],
                ),
                mock.patch.object(CAPTURE, "Capture", return_value=capture),
                mock.patch.object(CAPTURE, "request_stop") as stop,
                mock.patch.object(CAPTURE.signal, "signal"),
                mock.patch.object(CAPTURE.sys, "stderr", new_callable=io.StringIO),
            ):
                self.assertEqual(CAPTURE.main(), 1)
            self.assertFalse(capture.claimed)
            stop.assert_not_called()

    def test_wrong_context_stops_before_proof_mutations(self):
        capture = CAPTURE.Capture(self.args())
        capture.command = mock.Mock(return_value=b"different-cluster")
        with self.assertRaisesRegex(CAPTURE.CaptureError, "context"):
            capture.require_current_context()

    def test_missing_prometheus_series_is_not_zero(self):
        capture = CAPTURE.Capture(self.args())
        capture.http = mock.Mock(
            return_value={"status": "success", "data": {"result": []}}
        )
        with self.assertRaisesRegex(CAPTURE.CaptureError, "unique sample"):
            capture.metric("query")

    def test_history_cannot_pass_on_another_event_or_duplicate_coordinates(self):
        value = {
            "event_id": "event",
            "attempts": [
                {
                    "subscription_id": 1,
                    "attempt_number": 1,
                    "subscription_url": "http://sink/hooks/ok",
                    "outcome": "delivered",
                },
                {
                    "subscription_id": 2,
                    "attempt_number": 5,
                    "subscription_url": "http://sink/hooks/flaky",
                    "outcome": "exhausted",
                },
            ],
        }
        CAPTURE.assert_attempts(value, "event")
        with self.assertRaises(CAPTURE.CaptureError):
            CAPTURE.assert_attempts(value, "old-event")
        value["attempts"].append({**value["attempts"][0], "outcome": "retrying"})
        with self.assertRaisesRegex(CAPTURE.CaptureError, "duplicate"):
            CAPTURE.assert_attempts(value, "event")

    def test_trace_requires_all_persisted_attempt_coordinates(self):
        attrs = [{"key": "relay.event.id", "value": {"stringValue": "event"}}]
        spans = [
            {"name": n, "attributes": attrs}
            for n in ("relay.ingest", "kafka.produce", "relay.consume")
        ]
        attempt = {"subscription_id": 2, "attempt_number": 5}
        trace = {"batches": [{"scopeSpans": [{"spans": spans}]}]}
        with self.assertRaises(CAPTURE.CaptureError):
            CAPTURE.assert_trace(trace, "event", [attempt])
        spans.append(
            {
                "name": "relay.webhook.attempt",
                "attributes": [
                    *attrs,
                    {"key": "relay.subscription.id", "value": {"intValue": "2"}},
                    {"key": "relay.delivery.attempt", "value": {"intValue": "5"}},
                ],
            }
        )
        CAPTURE.assert_trace(trace, "event", [attempt])


if __name__ == "__main__":
    unittest.main()
