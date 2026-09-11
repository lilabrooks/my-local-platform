#!/usr/bin/env python3
"""Execute the fixed relay proof on an explicit cluster, with bounded exports."""

from __future__ import annotations

import argparse
import base64
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import datetime as dt
import importlib.util
import hashlib
import json
import math
import os
from pathlib import Path
import re
import secrets
import signal
import socket
import subprocess
import sys
import time
import threading
import urllib.error
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parent.parent
DELIVERIES = "mlp.relay.deliveries"
DLQ = DELIVERIES + ".dlq"
PAUSE = "autoscaling.keda.sh/paused-replicas"
PROOF_SECONDS = 1200
# These include the waits in the wrapped tools plus command overhead.
INSTALL_TIMEOUTS = {
    "aws-runtime-bootstrap": 330,
    "keda-install": 660,
    "monitoring-install-aws": 660,
    "argocd-install-aws": 1860,
}
CAPTURE_FILES = (
    "10-event.json",
    "11-attempts.json",
    "12-metrics.txt",
    "13-trace.json",
    "14-keda.txt",
    "15-dlq.json",
    "16-replay.json",
    "application-logs.txt",
)
QUERIES = {
    "lag": 'max(relay_consumer_group_lag_total{group="relay-deliver"})',
    "members": 'max(relay_group_members{group="relay-deliver"})',
    "unassigned": 'max(relay_topic_partitions_unassigned{group="relay-deliver"})',
    "idle": 'max(relay_group_unassigned_members{group="relay-deliver"})',
    "replicas": 'kube_deployment_spec_replicas{namespace="mlp",deployment="relay-deliver"}',
}
# Relay ServiceMonitor: 15s. Chart-managed kube-state-metrics: 30s.
# Allow two scrape intervals without changing the monitored configuration.
SAMPLE_MAX_AGE = {
    "lag": 30,
    "members": 30,
    "unassigned": 30,
    "idle": 30,
    "replicas": 60,
}


class CaptureError(RuntimeError):
    """The fixed live proof did not pass."""


class ObservationPending(CaptureError):
    """A fresh scrape or broker observation is temporarily unavailable."""


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def load_script(name: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / f"{name}.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def utc() -> str:
    return (
        dt.datetime.now(dt.UTC)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def request_stop(run_id: str) -> bool:
    # Workers do not receive Python signals. On the main thread, defer further
    # operator interrupts while the bounded, isolated stop request completes.
    handlers = {}
    if threading.current_thread() is threading.main_thread():
        for sig in (signal.SIGINT, signal.SIGTERM):
            handlers[sig] = signal.signal(sig, signal.SIG_IGN)
    try:
        subprocess.run(
            ["make", "aws-live-stop", f"AWS_RUN_ID={run_id}"],
            cwd=ROOT,
            timeout=30,
            check=True,
            start_new_session=True,
        )
        print("Controller stop request accepted.", file=sys.stderr)
        return True
    except (OSError, subprocess.SubprocessError):
        print(
            "Stop request failed; the foreground controller still owns the deadline. Request cleanup there now.",
            file=sys.stderr,
        )
        return False
    finally:
        for sig, handler in handlers.items():
            signal.signal(sig, handler)


def assert_trace(trace: dict, event_id: str, attempts: list[dict]) -> None:
    names = Counter()
    observed = Counter()
    for batch in trace.get("batches", trace.get("resourceSpans", [])):
        for scope in batch.get("scopeSpans", []):
            for span in scope.get("spans", []):
                attributes = {a["key"]: a["value"] for a in span.get("attributes", [])}
                if attributes.get("relay.event.id", {}).get("stringValue") != event_id:
                    continue
                names[span["name"]] += 1
                if span["name"] == "relay.webhook.attempt":
                    try:
                        key = (
                            int(attributes["relay.subscription.id"]["intValue"]),
                            int(attributes["relay.delivery.attempt"]["intValue"]),
                        )
                    except (KeyError, ValueError) as error:
                        raise CaptureError(
                            "trace attempt has incomplete attributes"
                        ) from error
                    observed[key] += 1
    expected = Counter((a["subscription_id"], a["attempt_number"]) for a in attempts)
    if (
        not expected
        or observed != expected
        or any(
            names[n] == 0 for n in ("relay.ingest", "kafka.produce", "relay.consume")
        )
    ):
        raise CaptureError(
            "trace does not join every persisted attempt for the selected event"
        )


def assert_attempts(value: dict, event_id: str) -> list[dict]:
    attempts = value.get("attempts", [])
    if value.get("event_id") != event_id:
        raise CaptureError("attempt history belongs to another event")
    healthy = [
        a
        for a in attempts
        if a["subscription_url"].endswith("/hooks/ok") and a["outcome"] == "delivered"
    ]
    failed = [
        a
        for a in attempts
        if a["subscription_url"].endswith("/hooks/flaky")
        and a["outcome"] == "exhausted"
    ]
    if len(healthy) != 1 or len(failed) != 1:
        raise CaptureError("selected event lacks healthy and exhausted outcomes")
    keys = [(a["subscription_id"], a["attempt_number"]) for a in attempts]
    if len(set(keys)) != len(keys):
        raise CaptureError("duplicate persisted attempt coordinates")
    return attempts


class Capture:
    def __init__(self, args):
        self.args = args
        self.evidence = load_script("m4-evidence")
        self.raw = (
            ROOT
            / ".evidence"
            / ("m4" if args.environment == "aws" else "m4-local")
            / args.run_id
        )
        if not re.fullmatch(r"[0-9]{8}T[0-9]{6}Z", args.run_id) or not re.fullmatch(
            r"[0-9a-f]{40}", args.commit
        ):
            raise CaptureError("canonical run ID and full commit are required")
        self.evidence.validate_run_id(args.run_id)
        self.evidence.ensure_beneath(self.raw, ROOT)
        self.deadline = time.monotonic() + PROOF_SECONDS
        self.claimed = False
        self.forwards = []
        self.urls = {}
        self.image = args.image
        self.started = utc()
        self.environment = os.environ.copy()
        self.environment["KUBECONFIG"] = str(Path.home() / ".kube/config")
        self.environment["HELM_KUBECONTEXT"] = args.context
        if args.environment == "aws":
            packet = self.evidence.read_json_object(self.raw / "06-go-no-go.json", "GO")
            self.environment = {
                key: self.environment[key]
                for key in ("HOME", "PATH", "TMPDIR", "KUBECONFIG", "HELM_KUBECONTEXT")
                if key in self.environment
            }
            self.environment.update(
                AWS_PROFILE=packet["aws_profile"],
                AWS_REGION="us-east-1",
                AWS_DEFAULT_REGION="us-east-1",
                AWS_PAGER="",
            )
            self.environment["KUBECONFIG"] = str(self.raw / "kubeconfig")
        self.stop_requested = False
        self.stop_attempted = False
        self.stop_lock = threading.Lock()
        self.cancelled = threading.Event()
        self.scanner = None
        self.clean = False
        self.changed_sink = False
        self.changed_pause = False
        self.provenance = {}

    def guard(self):
        if self.cancelled.is_set():
            raise CaptureError("capture has stopped")
        if time.monotonic() >= self.deadline:
            raise CaptureError("capture deadline reached")
        if self.args.environment == "aws":
            state = self.evidence.read_json_object(
                self.raw / "controller-state.json", "controller"
            )
            self.evidence.validate_live_controller(
                ROOT,
                self.args.run_id,
                self.args.commit,
                state.get("controller_pid", 0),
                phase="live",
            )
            if state.get("phase") != "live" or state.get("apply_exit") != 0:
                raise CaptureError(
                    "controller is no longer live after a successful apply"
                )
            session = self.evidence.read_json_object(
                self.raw / "00-session.json", "session"
            )
            if dt.datetime.now(dt.UTC) >= self.evidence.parse_utc(
                session["destroy_deadline"], "destroy deadline"
            ):
                raise CaptureError("destroy deadline reached")

    def command(self, arguments, *, data=None, timeout=60):
        self.guard()
        process = subprocess.Popen(
            arguments,
            cwd=ROOT,
            env=self.environment,
            stdin=subprocess.PIPE if data is not None else subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
        end = time.monotonic() + timeout
        try:
            first = True
            while True:
                self.guard()
                if time.monotonic() >= end:
                    raise CaptureError(f"{arguments[0]} timed out")
                try:
                    stdout, _ = process.communicate(
                        input=data if first else None,
                        timeout=min(1, end - time.monotonic()),
                    )
                    break
                except subprocess.TimeoutExpired:
                    first = False
            if process.returncode:
                # Diagnostics may contain Kubernetes Secret data. Keep them off disk.
                raise CaptureError(
                    f"{arguments[0]} failed with status {process.returncode}"
                )
            if len(stdout) > 8 * 1024 * 1024:
                raise CaptureError("command output exceeds evidence limit")
            return stdout
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=2)

    def kubectl(self, *arguments, **kwargs):
        return self.command(
            [
                "kubectl",
                "--context",
                self.args.context,
                "--request-timeout=15s",
                *arguments,
            ],
            **kwargs,
        )

    def get(self, namespace, resource):
        return json.loads(self.kubectl("-n", namespace, "get", resource, "-o", "json"))

    def wait_for_resource(self, namespace, resource):
        def exists():
            output = self.kubectl(
                "-n", namespace, "get", resource, "--ignore-not-found", "-o", "json"
            )
            return json.loads(output) if output.strip() else None

        return self.wait(exists, 600)

    def set_phase_deadline(self, *, deployment=False):
        now = time.monotonic()
        if self.args.environment == "aws":
            session = self.evidence.read_json_object(
                self.raw / "00-session.json", "session"
            )
            remaining = (
                self.evidence.parse_utc(session["destroy_deadline"], "destroy deadline")
                - dt.datetime.now(dt.UTC)
            ).total_seconds()
            allowance = (
                remaining - PROOF_SECONDS
                if deployment
                else min(PROOF_SECONDS, remaining)
            )
            if allowance <= 0:
                raise CaptureError(
                    "insufficient session time for deployment and proof reserve"
                )
            self.deadline = now + allowance
        else:
            self.deadline = now + PROOF_SECONDS

    def write(self, name, value):
        if self.scanner:
            self.evidence.scan_secret_bytes(
                json.dumps(value).encode(), self.scanner, name
            )
        self.raw.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.evidence.write_json_exclusive(self.raw / name, value, 0o600)

    def wait(self, probe, seconds=90):
        end = min(self.deadline, time.monotonic() + seconds)
        while True:
            self.guard()
            if time.monotonic() >= end:
                raise CaptureError(
                    "expected observation did not arrive before its deadline"
                )
            value = probe()
            self.guard()
            if time.monotonic() >= end:
                raise CaptureError(
                    "expected observation did not arrive before its deadline"
                )
            if value:
                return value
            time.sleep(min(1, max(0, end - time.monotonic())))

    def http(self, service, path, body=None, *, headers=None, status=200):
        self.guard()
        url = self.urls[service] + path
        request = urllib.request.Request(
            url,
            data=None if body is None else json.dumps(body).encode(),
            headers={"Content-Type": "application/json", **(headers or {})},
        )
        try:
            with urllib.request.build_opener(NoRedirect()).open(
                request, timeout=10
            ) as response:
                if response.status != status:
                    raise CaptureError(f"unexpected HTTP status from {service}")
                data = response.read(8 * 1024 * 1024 + 1)
        except urllib.error.HTTPError as error:
            raise CaptureError(f"{service} returned HTTP {error.code}") from None
        if len(data) > 8 * 1024 * 1024:
            raise CaptureError("HTTP evidence exceeds size limit")
        return json.loads(data)

    def forward(self, service, namespace, resource, remote_port):
        override = getattr(self.args, service + "_url", None)
        if override:
            parsed = urllib.parse.urlsplit(override)
            if (
                parsed.scheme != "http"
                or parsed.hostname not in {"127.0.0.1", "localhost"}
                or parsed.username
                or parsed.query
                or parsed.fragment
            ):
                raise CaptureError(
                    "explicit evidence URLs must name a local port-forward"
                )
            self.urls[service] = override.rstrip("/")
            return
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        self.guard()
        process = subprocess.Popen(
            [
                "kubectl",
                "--context",
                self.args.context,
                "-n",
                namespace,
                "port-forward",
                "--address=127.0.0.1",
                resource,
                f"{port}:{remote_port}",
            ],
            env=self.environment,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self.forwards.append(process)
        self.urls[service] = f"http://127.0.0.1:{port}"

        def ready():
            if process.poll() is not None:
                raise CaptureError(f"{service} port-forward exited")
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=1):
                    return True
            except OSError:
                return False

        self.wait(ready, 30)

    def job(
        self,
        operation,
        *,
        topic=DELIVERIES,
        starts=None,
        marker="",
        event_id="",
        replay=False,
    ):
        self.guard()
        command = (
            ["/relay-replay", "--since", self.started]
            if replay
            else [
                "/relay-capture",
                "--operation",
                operation,
                "--topic",
                topic,
                "--starts",
                json.dumps(starts or {}),
                "--marker",
                marker,
                "--event-id",
                event_id,
            ]
        )
        container = {
            "name": "capture",
            "image": self.image,
            "command": command,
            "envFrom": [{"configMapRef": {"name": "relay-runtime"}}],
            "securityContext": {
                "allowPrivilegeEscalation": False,
                "readOnlyRootFilesystem": True,
                "capabilities": {"drop": ["ALL"]},
            },
            "resources": {
                "requests": {"cpu": "25m", "memory": "32Mi"},
                "limits": {"cpu": "250m", "memory": "128Mi"},
            },
        }
        if operation == "database":
            container["env"] = [
                {
                    "name": "DATABASE_URL",
                    "valueFrom": {
                        "secretKeyRef": {"name": "relay-secrets", "key": "DATABASE_URL"}
                    },
                }
            ]
        doc = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {"generateName": "relay-proof-", "namespace": "mlp"},
            "spec": {
                "backoffLimit": 0,
                "activeDeadlineSeconds": 100,
                "ttlSecondsAfterFinished": 600,
                "template": {
                    "spec": {
                        "serviceAccountName": (
                            "relay-deliver"
                            if self.args.environment == "aws"
                            else "default"
                        )
                        if replay
                        else "relay-capture",
                        "restartPolicy": "Never",
                        "securityContext": {
                            "runAsNonRoot": True,
                            "runAsUser": 65532,
                            "seccompProfile": {"type": "RuntimeDefault"},
                        },
                        "containers": [container],
                    }
                },
            },
        }
        name = (
            self.kubectl(
                "-n",
                "mlp",
                "create",
                "-f",
                "-",
                "-o",
                "name",
                data=json.dumps(doc).encode(),
            )
            .decode()
            .strip()
        )
        if not re.fullmatch(r"job.batch/relay-proof-[a-z0-9-]+", name):
            raise CaptureError("unexpected capture Job name")

        def completed():
            status = self.get("mlp", name).get("status", {})
            if status.get("failed") or any(
                c.get("type") == "Failed" and c.get("status") == "True"
                for c in status.get("conditions", [])
            ):
                raise CaptureError(f"{operation} Job failed")
            return status.get("succeeded", 0) == 1

        self.wait(completed, 110)
        output = self.kubectl("-n", "mlp", "logs", name).decode()
        return output if replay else json.loads(output)

    def metric(self, query):
        value = self.http(
            "prometheus", "/api/v1/query?" + urllib.parse.urlencode({"query": query})
        )
        results = value.get("data", {}).get("result", [])
        if value.get("status") != "success" or len(results) != 1:
            raise ObservationPending("Prometheus did not return a unique sample")
        number = float(results[0]["value"][1])
        if not math.isfinite(number) or number < 0:
            raise CaptureError("invalid Prometheus value")
        return number

    def sample(self):
        freshness = self.metric(
            'time() - min(relay_lag_refreshed_timestamp_seconds and on(instance) relay_build_info{role="ingest"})'
        )
        if freshness > 30:
            raise ObservationPending("broker lag snapshot is stale")
        for name, query in QUERIES.items():
            # timestamp(max(...)) reports evaluation time, not scrape age.
            source = query[4:-1] if query.startswith("max(") else query
            if self.metric(f"time() - min(timestamp({source}))") > SAMPLE_MAX_AGE[name]:
                raise ObservationPending("proof metric sample is stale")
        return {
            "captured_at": utc(),
            **{key: self.metric(query) for key, query in QUERIES.items()},
        }

    def pending_sample(self):
        try:
            return self.sample()
        except ObservationPending:
            # No stale value can satisfy a terminal condition. Callers retain
            # their original deadline and retry cadence, including on None.
            return None

    def setup(self):
        self.set_phase_deadline(deployment=self.args.deploy)
        self.guard()
        if (
            self.command(["git", "rev-parse", "HEAD"]).decode().strip()
            != self.args.commit
        ):
            raise CaptureError("capture checkout does not match the candidate")
        if self.args.environment == "aws":
            if self.command(
                ["git", "status", "--porcelain", "--untracked-files=all"]
            ).strip():
                raise CaptureError("live capture requires a clean checkout")
            packet = self.evidence.read_json_object(self.raw / "06-go-no-go.json", "GO")
            self.image = packet["image_references"]["relay"]
        if not self.image:
            raise CaptureError("local rehearsal requires the candidate relay image")
        self.clean = not self.command(
            ["git", "status", "--porcelain", "--untracked-files=all"]
        ).strip()
        if self.args.deploy:
            if self.args.environment != "aws":
                raise CaptureError(
                    "deployment workflow is only for the reviewed AWS runtime"
                )
            # The bootstrap and Make's restricted AWS environment share this file.
            common = [
                f"AWS_RUN_ID={self.args.run_id}",
                f"AWS_APPROVED_COMMIT={self.args.commit}",
                f"AWS_PROFILE_NAME={packet['aws_profile']}",
                f"AWS_KUBECONFIG={self.environment['KUBECONFIG']}",
                f"AWS_KUBE_CONTEXT={self.args.context}",
            ]
            self.command(["make", "aws-kubeconfig", *common])
            self.require_current_context()
            output = (
                self.aws_command(
                    [
                        "terraform",
                        "-chdir=infra/terraform/envs/dev",
                        "output",
                        "-raw",
                        "msk_bootstrap_brokers",
                    ]
                )
                .decode()
                .strip()
            )
            self.command(
                [
                    "make",
                    "aws-k8s-render",
                    *common,
                    f"AWS_RELAY_IMAGE={self.image}",
                    f"AWS_SINK_IMAGE={packet['image_references']['sink']}",
                    f"AWS_MSK_BOOTSTRAP={output}",
                ],
                timeout=180,
            )
            for target, timeout in INSTALL_TIMEOUTS.items():
                self.require_current_context()
                self.command(
                    [
                        "make",
                        target,
                        *common,
                        f"AWS_ROOT_APPLICATION={self.raw / 'rendered/root-app.json'}",
                        "ASSUME_YES=1",
                    ],
                    timeout=timeout,
                )
        if self.args.environment == "aws":
            self.scanner = self.evidence.load_secret_scan(self.raw, self.args.run_id)
        self.require_current_context()
        if self.args.environment == "local":
            provenance = load_script("verify-k8s-sigterm")
            provenance.CONTEXT = self.args.context
            demo = load_script("m4-local-demo")
            self.provenance = demo.collect_provenance(provenance, self.args.commit)
            deployment = self.get("mlp", "deployment/relay-ingest")
            if (
                deployment["spec"]["template"]["spec"]["containers"][0]["image"]
                != self.image
            ):
                raise CaptureError("capture image differs from the running candidate")
        if self.args.environment == "aws":
            # Match every running workload image to the staged references.
            for app, service in (
                ("relay-ingest", "relay"),
                ("relay-deliver", "relay"),
                ("sink", "sink"),
            ):
                self.wait_for_resource("mlp", f"deployment/{app}")
                self.kubectl(
                    "-n",
                    "mlp",
                    "rollout",
                    "status",
                    f"deployment/{app}",
                    "--timeout=180s",
                    timeout=190,
                )
                deployment = self.get("mlp", f"deployment/{app}")
                if (
                    deployment["spec"]["template"]["spec"]["containers"][0]["image"]
                    != packet["image_references"][service]
                ):
                    raise CaptureError("deployed image differs from approved digest")
            self.wait_for_resource("mlp", "deployment/tempo")
            self.kubectl(
                "-n",
                "mlp",
                "rollout",
                "status",
                "deployment/tempo",
                "--timeout=180s",
                timeout=190,
            )
        self.kubectl(
            "apply",
            "-f",
            "-",
            data=json.dumps(
                {
                    "apiVersion": "v1",
                    "kind": "ServiceAccount",
                    "metadata": {"name": "relay-capture", "namespace": "mlp"},
                }
            ).encode(),
        )
        for service, namespace, resource, port in (
            ("ingest", "mlp", "service/relay-ingest", 80),
            ("sink", "mlp", "service/sink", 8081),
            (
                "prometheus",
                "monitoring",
                "service/monitoring-kube-prometheus-prometheus",
                9090,
            ),
            ("tempo", "mlp", "service/tempo", 3200),
        ):
            if self.args.environment == "aws":
                self.wait_for_resource(namespace, resource)
            self.forward(service, namespace, resource, port)

    def drained_sample(self):
        sample = self.pending_sample()
        return (
            sample
            if (
                sample is not None
                and sample["lag"] == 0
                and sample["replicas"] == 1
                and sample["members"] == 1
            )
            else None
        )

    def baseline(self):
        return self.wait(self.drained_sample, 180)

    @contextmanager
    def load_pool(self):
        with ThreadPoolExecutor(max_workers=8) as pool:
            try:
                yield pool
            except BaseException:
                # Catch in the executor's scope, BEFORE __exit__ waits for
                # queued work. Active HTTP requests retain their 10s bound.
                self.cancelled.set()
                self.stop()
                pool.shutdown(wait=False, cancel_futures=True)
                raise

    def require_current_context(self):
        actual = self.command(["kubectl", "config", "current-context"]).decode().strip()
        if actual != self.args.context:
            raise CaptureError("current context differs from explicit proof context")
        if self.args.environment == "aws":
            config = json.loads(
                self.kubectl("config", "view", "--minify", "-o", "json")
            )
            cluster = (
                self.aws_command(
                    [
                        "terraform",
                        "-chdir=infra/terraform/envs/dev",
                        "output",
                        "-raw",
                        "eks_cluster_name",
                    ]
                )
                .decode()
                .strip()
            )
            state = (
                self.aws_command(
                    [
                        "aws",
                        "eks",
                        "describe-cluster",
                        "--name",
                        cluster,
                        "--query",
                        "cluster.endpoint",
                        "--output",
                        "text",
                    ]
                )
                .decode()
                .strip()
            )
            if config["clusters"][0]["cluster"]["server"] != state:
                raise CaptureError(
                    "proof context does not match Terraform EKS endpoint"
                )

    def aws_command(self, arguments):
        packet = self.evidence.read_json_object(self.raw / "06-go-no-go.json", "GO")
        return self.command(
            [
                "env",
                "-i",
                *[
                    f"{name}={os.environ[name]}"
                    for name in ("HOME", "PATH", "TMPDIR")
                    if name in os.environ
                ],
                f"AWS_PROFILE={packet['aws_profile']}",
                "AWS_REGION=us-east-1",
                "AWS_DEFAULT_REGION=us-east-1",
                "AWS_PAGER=",
                *arguments,
            ]
        )

    def proof(self):
        print("capture: baseline, idempotency and persisted attempts", flush=True)
        controls = self.http("sink", "/control")
        if (
            controls["latency_ms"] != 0
            or controls["fail_rate"] != 0
            or controls.get("latched")
        ):
            raise CaptureError("sink is not at the baseline")
        scaled = self.get("mlp", "scaledobject/relay-deliver")
        if PAUSE in scaled.get("metadata", {}).get("annotations", {}):
            raise CaptureError("KEDA is already paused")
        baseline = self.baseline()
        starts = self.job("snapshot")
        dlq_start = self.job("snapshot", topic=DLQ)
        marker = "m4-" + self.args.run_id + "-" + secrets.token_hex(6)
        trace_id = secrets.token_hex(16)
        request = {
            "tenant_id": "acme",
            "type": "m4.proof",
            "data": {"marker": marker},
            "idempotency_key": marker,
        }
        headers = {"traceparent": f"00-{trace_id}-{secrets.token_hex(8)}-01"}
        accepted = self.http(
            "ingest", "/v1/events", request, headers=headers, status=202
        )
        repeated = self.http("ingest", "/v1/events", request, status=202)
        event_id = accepted.get("id", "")
        if (
            not re.fullmatch(r"evt_[0-9a-f]{32}", event_id)
            or repeated.get("id") != event_id
        ):
            raise CaptureError(
                "idempotent repeat returned a different or invalid event ID"
            )

        def finished():
            value = self.http("ingest", f"/v1/events/{event_id}/attempts")
            outcomes = {a["outcome"] for a in value.get("attempts", [])}
            return value if {"delivered", "exhausted"} <= outcomes else None

        history = self.wait(finished)
        attempts = assert_attempts(history, event_id)
        matching = [
            d
            for d in self.http("sink", "/received")["deliveries"]
            if d["webhook_id"] == event_id
            and d["path"] == "/hooks/ok"
            and 200 <= d["status"] < 300
        ]
        if (
            len(matching) != 1
            or matching[0]["data"] != request["data"]
            or matching[0]["type"] != request["type"]
        ):
            raise CaptureError(
                "sink did not record exactly one matching signed healthy delivery"
            )
        durable = self.job("database", event_id=event_id, marker=marker)
        records = self.job("read", starts=starts)
        copies = [
            r
            for r in records["records"]
            if json.loads(base64.b64decode(r["value"])).get("id") == event_id
        ]
        if len(copies) != 1:
            raise CaptureError(
                "idempotent event did not produce exactly one Kafka record"
            )
        self.write(
            "10-event.json",
            {
                "request": request,
                "accepted": accepted,
                "repeated": repeated,
                "sink": matching,
                "database": durable,
                "kafka": copies,
                "trace_id": trace_id,
            },
        )
        self.write("11-attempts.json", history)

        def trace_ready():
            try:
                value = self.http("tempo", f"/api/traces/{trace_id}")
                assert_trace(value, event_id, attempts)
                return value
            except CaptureError:
                return None

        trace = self.wait(trace_ready)
        self.write("13-trace.json", trace)
        print("capture: fixed 600-event scale cohort", flush=True)
        # Only demo tenants affect the scale cohort; acme failures are separate.
        self.changed_sink = True
        self.http("sink", "/control", {"latency_ms": 1000, "fail_rate": 0})
        load_ids = []

        def post(i):
            body = {
                "tenant_id": f"demo-{i % 16 + 1:02d}",
                "type": "m4.load",
                "data": {"cohort": marker, "sequence": i},
                "idempotency_key": f"{marker}-{i}",
            }
            try:
                return self.http("ingest", "/v1/events", body, status=202)["id"]
            except BaseException:
                self.cancelled.set()
                self.stop()
                raise

        samples = [baseline]
        with self.load_pool() as pool:
            futures = [pool.submit(post, i) for i in range(600)]
            released = False
            end = time.monotonic() + 480
            while True:
                self.guard()
                if time.monotonic() >= end:
                    raise CaptureError(
                        "fixed load did not scale, drain and return to one"
                    )
                for future in futures:
                    if future.done() and future.exception():
                        self.stop()
                        raise CaptureError(
                            "load producer failed"
                        ) from future.exception()
                sample = self.pending_sample()
                self.guard()
                if time.monotonic() >= end:
                    raise CaptureError(
                        "fixed load did not scale, drain and return to one"
                    )
                if sample is not None:
                    samples.append(sample)
                if (
                    sample is not None
                    and all(f.done() for f in futures)
                    and sample["lag"] == 0
                    and not released
                ):
                    self.http("sink", "/control", {"latency_ms": 0, "fail_rate": 0})
                    released = True
                if (
                    sample is not None
                    and all(f.done() for f in futures)
                    and released
                    and sample["lag"] == 0
                    and sample["replicas"] == 1
                    and sample["members"] == 1
                ):
                    load_ids = [f.result() for f in futures]
                    break
                time.sleep(3)
        if (
            len(set(load_ids)) != 600
            or max(s["lag"] for s in samples) <= 0
            or max(s["replicas"] for s in samples) <= 1
            or max(s["members"] for s in samples) <= 1
        ):
            raise CaptureError("load cohort or backlog observation is incomplete")
        self.write(
            "12-metrics.txt",
            {
                "queries": QUERIES,
                "sample_max_age_seconds": SAMPLE_MAX_AGE,
                "samples": samples,
                "load_event_ids": load_ids,
                "cohort": marker,
            },
        )
        self.write(
            "14-keda.txt",
            {
                "samples": samples,
                "scaledobject": self.get("mlp", "scaledobject/relay-deliver"),
            },
        )
        poison = self.job("poison", marker=marker)

        def dead_letters():
            value = self.job("read", topic=DLQ, starts=dlq_start)
            decoded = [
                json.loads(base64.b64decode(r["value"])) for r in value["records"]
            ]
            event = [d for d in decoded if d.get("record", {}).get("id") == event_id]
            bad = [
                d
                for d in decoded
                if all(
                    d.get("source", {}).get(k) == poison[k]
                    for k in ("topic", "partition", "offset")
                )
            ]
            if not event or not bad:
                return None
            if (
                len(event) != 1
                or len(bad) != 1
                or not event[0].get("url", "").endswith("/hooks/flaky")
                or event[0].get("attempts") != 5
            ):
                raise CaptureError("unexpected exhausted or poison DLQ cohort")
            source = bad[0]["source"]
            if (
                source["raw_value"] != poison["value"]
                or source["key"] != poison["key"]
                or source["timestamp"] != poison["timestamp"]
                or source["raw_value_truncated"]
                or source["key_truncated"]
                or source["key_sha256"]
                != hashlib.sha256(base64.b64decode(poison["key"])).hexdigest()
                or source["original_key_bytes"] != len(base64.b64decode(poison["key"]))
                or source["original_value_bytes"]
                != len(base64.b64decode(poison["value"]))
            ):
                raise CaptureError("poison source bytes or coordinates changed")
            return {
                "event_id": event_id,
                "exhausted": event,
                "poison": poison,
                "dead_letters": bad,
                "broker": value,
            }

        self.write("15-dlq.json", self.wait(dead_letters, 120))
        before = sum(
            d["webhook_id"] == event_id and d["path"] == "/hooks/ok"
            for d in self.http("sink", "/received")["deliveries"]
        )
        self.changed_pause = True
        self.kubectl(
            "-n",
            "mlp",
            "annotate",
            "scaledobject",
            "relay-deliver",
            PAUSE + "=0",
            "--overwrite",
        )
        self.wait(
            lambda: (
                not json.loads(
                    self.kubectl(
                        "-n",
                        "mlp",
                        "get",
                        "pods",
                        "-l",
                        "app.kubernetes.io/name=relay-deliver",
                        "-o",
                        "json",
                    )
                )["items"]
            ),
            120,
        )
        replay = self.job("replay", replay=True)
        self.kubectl(
            "-n", "mlp", "annotate", "scaledobject", "relay-deliver", PAUSE + "-"
        )

        def replayed():
            records = [
                d
                for d in self.http("sink", "/received")["deliveries"]
                if d["webhook_id"] == event_id
                and d["path"] == "/hooks/ok"
                and 200 <= d["status"] < 300
            ]
            return records if len(records) > before else None

        replay_deliveries = self.wait(replayed, 120)

        final_state = self.wait(self.drained_sample, 120)
        self.write(
            "16-replay.json",
            {
                "event_id": event_id,
                "since": self.started,
                "offset_reset": replay,
                "before": before,
                "deliveries": replay_deliveries,
                "final_state": final_state,
            },
        )
        logs = self.kubectl(
            "-n",
            "mlp",
            "logs",
            "-l",
            "app.kubernetes.io/name in (relay-ingest,relay-deliver,sink)",
            "--all-containers",
            "--prefix",
            "--tail=-1",
            f"--since-time={self.started}",
        )
        if self.scanner:
            self.evidence.scan_secret_bytes(logs, self.scanner, "application-logs.txt")
        self.write("application-logs.txt", {"logs": logs.decode()})

    def stop(self):
        with self.stop_lock:
            if self.args.environment != "aws" or self.stop_attempted:
                return
            self.stop_attempted = True
            self.stop_requested = request_stop(self.args.run_id)

    def cleanup_local(self):
        # A timed-out write may have taken effect. Try both restorations, then
        # read the controls back; successful commands alone are not proof.
        errors = []
        if self.changed_sink:
            try:
                self.http("sink", "/control", {"latency_ms": 0, "fail_rate": 0})
            except (RuntimeError, OSError, ValueError) as error:
                errors.append(error)
        if self.changed_pause:
            try:
                self.kubectl(
                    "-n",
                    "mlp",
                    "annotate",
                    "scaledobject",
                    "relay-deliver",
                    PAUSE + "-",
                )
            except (RuntimeError, OSError, ValueError) as error:
                errors.append(error)
        try:
            controls = self.http("sink", "/control")
            annotations = self.get("mlp", "scaledobject/relay-deliver")["metadata"].get(
                "annotations", {}
            )
            verified = (
                controls["latency_ms"] == 0
                and controls["fail_rate"] == 0
                and not controls.get("latched")
                and PAUSE not in annotations
            )
        except (RuntimeError, OSError, ValueError, KeyError):
            verified = False
        return verified and not errors

    def run(self):
        # Claim before any setup mutation. A failed attempt cannot reuse evidence.
        self.write(
            "capture-start.json",
            {
                "run_id": self.args.run_id,
                "source_commit": self.args.commit,
                "started_at": self.started,
            },
        )
        self.claimed = True
        result = "failed"
        try:
            self.setup()
            self.set_phase_deadline()
            self.proof()
            result = "passed"
        except BaseException:
            self.cancelled.set()
            self.stop()  # Ask for destroy before local cleanup or analysis.
            raise
        finally:
            # AWS resource cleanup belongs to the controller, never this receipt.
            cleanup = False
            if self.args.environment == "local":
                # Cleanup has its own short allowance, even after capture expiry.
                self.cancelled.clear()
                self.deadline = time.monotonic() + 30
                try:
                    cleanup = self.cleanup_local()
                    if not cleanup:
                        result = "failed"
                except (RuntimeError, OSError, ValueError):
                    cleanup = False
                    result = "failed"
                finally:
                    self.close_forwards()
            else:
                self.close_forwards()
            self.write(
                "capture-result.json",
                {
                    "schema_version": 1,
                    "run_id": self.args.run_id,
                    "source_commit": self.args.commit,
                    "worktree_clean": self.clean,
                    "environment": self.args.environment,
                    "provenance": self.provenance,
                    "files": {
                        name: self.evidence.hash_file(self.raw / name)
                        for name in CAPTURE_FILES
                        if (self.raw / name).is_file()
                    },
                    "cleanup_verified": cleanup,
                    "stop_request_attempted": self.stop_attempted,
                    "stop_request_accepted": self.stop_requested,
                    "result": result,
                    "started_at": self.started,
                    "finished_at": utc(),
                },
            )
        if result != "passed":
            raise CaptureError("capture cleanup did not pass")

    def close_forwards(self):
        for process in self.forwards:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=2)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--environment", choices=("aws", "local"), required=True)
    parser.add_argument("--context", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--image", default="")
    parser.add_argument("--deploy", action="store_true")
    parser.add_argument("--verify-output", choices=CAPTURE_FILES)
    for service in ("ingest", "sink", "prometheus", "tempo"):
        parser.add_argument(f"--{service}-url")
    args = parser.parse_args()

    def interrupted(signum, _frame):
        raise CaptureError(f"capture interrupted by signal {signum}")

    signal.signal(signal.SIGINT, interrupted)
    signal.signal(signal.SIGTERM, interrupted)
    capture = None
    try:
        capture = Capture(args)
        if args.verify_output:
            receipt = capture.evidence.read_json_object(
                capture.raw / "capture-result.json", "capture result"
            )
            if (
                receipt.get("result") != "passed"
                or receipt.get("source_commit") != args.commit
                or receipt.get("run_id") != args.run_id
                or receipt.get("environment") != args.environment
                or receipt.get("files", {}).get(args.verify_output)
                != capture.evidence.hash_file(capture.raw / args.verify_output)
            ):
                raise CaptureError("capture output is not a verified pass for this run")
            return 0
        capture.run()
    except (
        RuntimeError,
        OSError,
        ValueError,
        KeyError,
        subprocess.SubprocessError,
    ) as error:
        if args.environment == "aws" and not args.verify_output:
            if capture is not None and capture.claimed:
                capture.stop()
            elif (
                capture is None
                and re.fullmatch(r"[0-9]{8}T[0-9]{6}Z", args.run_id)
                and not (
                    ROOT / ".evidence" / "m4" / args.run_id / "capture-start.json"
                ).exists()
            ):
                # Initialization (for example a damaged GO receipt) can fail
                # before Capture.run has installed its failure transition.
                request_stop(args.run_id)
        detail = str(error) if isinstance(error, CaptureError) else type(error).__name__
        print(
            f"M4 capture failed: {detail}. See the explicit stop-request result above; read-only verification and duplicate attempts do not request cleanup.",
            file=sys.stderr,
        )
        return 1
    print("Machine captures passed.")
    if args.environment == "aws":
        print("Take the reviewed screenshots, then request aws-live-stop.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
