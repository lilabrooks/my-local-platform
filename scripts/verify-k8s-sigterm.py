#!/usr/bin/env python3
"""Prove relay ingest and delivery drain under minikube SIGTERM."""

from __future__ import annotations

import argparse
from concurrent.futures import Future, ThreadPoolExecutor
import datetime
import json
import os
from pathlib import Path
import re
import select
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from typing import Any
import urllib.error
import urllib.request


ROOT = Path(__file__).resolve().parent.parent
LOCAL_EVIDENCE_ROOT = ROOT / ".evidence" / "m4-local"
NAMESPACE = "mlp"
CONTEXT = "mlp"
POSTGRES_CONTAINER = "mlp-postgres"
POSTGRES_USER = "platform"
POSTGRES_DATABASE = "platform"
PAUSE_ANNOTATION = "autoscaling.keda.sh/paused-replicas"
REVISION_RE = re.compile(r"^[0-9a-f]{7,40}$")


class VerificationError(RuntimeError):
    """The controlled termination did not prove the shutdown contract."""


class VerificationRunError(VerificationError):
    """A controlled run failed after producing a cleanup receipt."""

    def __init__(self, receipt: dict[str, Any]):
        super().__init__(str(receipt.get("error", "verification failed")))
        self.receipt = receipt


def command(
    arguments: list[str], *, input_text: str | None = None, timeout: float = 30
) -> str:
    result = subprocess.run(
        arguments,
        cwd=ROOT,
        input=input_text,
        text=True,
        capture_output=True,
        check=False,
        timeout=timeout,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise VerificationError(f"{' '.join(arguments)} failed: {detail}")
    return result.stdout


def kubectl(*arguments: str, timeout: float = 30) -> str:
    return command(
        ["kubectl", "--context", CONTEXT, "-n", NAMESPACE, *arguments],
        timeout=timeout,
    )


def utc_now() -> str:
    return (
        datetime.datetime.now(datetime.UTC)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def http_json(
    method: str,
    url: str,
    payload: dict[str, Any] | None = None,
    *,
    timeout: float = 5,
) -> tuple[int, dict[str, Any]]:
    body = None
    headers: dict[str, str] = {}
    if payload is not None:
        body = json.dumps(payload, separators=(",", ":")).encode()
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            content = response.read()
            return response.status, json.loads(content) if content else {}
    except urllib.error.HTTPError as error:
        content = error.read()
        try:
            decoded = json.loads(content) if content else {}
        except json.JSONDecodeError:
            decoded = {"body": content.decode(errors="replace")}
        return error.code, decoded


def wait_http(url: str, timeout: float = 20) -> None:
    deadline = time.monotonic() + timeout
    last = "no response"
    while time.monotonic() < deadline:
        try:
            status, _ = http_json("GET", url, timeout=1)
            if status == 200:
                return
            last = f"HTTP {status}"
        except (OSError, ValueError) as error:
            last = str(error)
        time.sleep(0.1)
    raise VerificationError(f"{url} did not become reachable: {last}")


def readiness_is_false(url: str) -> bool:
    try:
        status, _ = http_json("GET", f"{url}/readyz", timeout=1)
    except (OSError, ValueError):
        return False
    return status == 503


def sink_control_state(url: str) -> dict[str, Any]:
    status, payload = http_json("GET", f"{url}/control")
    if status != 200:
        raise VerificationError(f"sink control state returned {status}: {payload}")
    return payload


def sink_is_at_baseline(url: str) -> bool:
    state = sink_control_state(url)
    return (
        state.get("latency_ms") == 0
        and state.get("fail_rate") == 0
        and state.get("latched") in (None, [])
        and not any(state.get("held", {}).values())
    )


class PortForward:
    def __init__(self, target: str, remote_port: int):
        self.local_port = free_port()
        self.process = subprocess.Popen(
            [
                "kubectl",
                "--context",
                CONTEXT,
                "-n",
                NAMESPACE,
                "port-forward",
                "--address",
                "127.0.0.1",
                target,
                f"{self.local_port}:{remote_port}",
            ],
            cwd=ROOT,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.local_port}"

    def stop(self) -> None:
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=3)


def ready_pods(application: str) -> list[dict[str, Any]]:
    payload = json.loads(
        kubectl(
            "get",
            "pods",
            "-l",
            f"app.kubernetes.io/name={application}",
            "-o",
            "json",
        )
    )
    result = []
    for pod in payload.get("items", []):
        if pod.get("metadata", {}).get("deletionTimestamp"):
            continue
        conditions = pod.get("status", {}).get("conditions", [])
        if any(
            condition.get("type") == "Ready" and condition.get("status") == "True"
            for condition in conditions
        ):
            result.append(pod)
    return result


def wait_ready_pods(
    application: str, count: int, timeout: float = 120
) -> list[dict[str, Any]]:
    deadline = time.monotonic() + timeout
    last = 0
    while time.monotonic() < deadline:
        pods = ready_pods(application)
        last = len(pods)
        if last == count:
            return pods
        time.sleep(1)
    raise VerificationError(f"{application} has {last} ready pods, want {count}")


def image_provenance(pod: dict[str, Any], source_commit: str) -> dict[str, str]:
    try:
        image_id = pod["status"]["containerStatuses"][0]["imageID"]
    except (KeyError, IndexError, TypeError) as error:
        raise VerificationError("ready pod has no running image id") from error
    reference = image_id.removeprefix("docker://")
    try:
        inspected = json.loads(
            command(
                ["docker", "exec", CONTEXT, "crictl", "inspecti", reference],
                timeout=30,
            )
        )
        revision = inspected["info"]["imageSpec"]["config"]["Labels"][
            "org.opencontainers.image.revision"
        ]
    except (json.JSONDecodeError, KeyError, TypeError) as error:
        raise VerificationError(
            f"running image {image_id} has no readable revision label"
        ) from error
    if not isinstance(revision, str) or not REVISION_RE.fullmatch(revision):
        raise VerificationError(
            f"running image {image_id} has invalid revision label {revision!r}"
        )
    if not source_commit.startswith(revision):
        raise VerificationError(
            f"running image revision {revision} does not match source commit {source_commit}"
        )
    return {"image_id": image_id, "image_revision": revision}


class PodWatch:
    template = (
        "{{.metadata.deletionTimestamp}}|{{range .status.conditions}}"
        '{{if eq .type "Ready"}}{{.status}}{{end}}{{end}}|'
        "{{range .status.containerStatuses}}{{if .state.terminated}}"
        "{{.state.terminated.exitCode}}|{{.state.terminated.signal}}|"
        "{{.state.terminated.reason}}{{else}}<no value>|<no value>|"
        '{{"<no value>"}}{{end}}{{end}}{{"\\n"}}'
    )

    def __init__(self, pod: str):
        self.events: list[dict[str, Any]] = []
        self.process = subprocess.Popen(
            [
                "kubectl",
                "--context",
                CONTEXT,
                "-n",
                NAMESPACE,
                "get",
                "pod",
                pod,
                "--watch",
                "-o",
                f"go-template={self.template}",
            ],
            cwd=ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        self.thread = threading.Thread(target=self._read, daemon=True)
        self.thread.start()

    def _read(self) -> None:
        assert self.process.stdout is not None
        for raw in self.process.stdout:
            fields = raw.rstrip("\n").split("|")
            if len(fields) != 5:
                continue
            deletion, ready, exit_code, signal, reason = fields
            self.events.append(
                {
                    "observed_at": time.monotonic(),
                    "deletion_timestamp": deletion,
                    "ready": ready,
                    "exit_code": exit_code,
                    "signal": signal,
                    "reason": reason,
                }
            )

    def stop(self) -> list[dict[str, Any]]:
        if self.process.poll() is None:
            self.process.terminate()
        try:
            self.process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=3)
        self.thread.join(timeout=2)
        return list(self.events)


def analyze_termination(
    events: list[dict[str, Any]],
    requested_at: float,
    readiness_false_at: float,
    grace_seconds: int,
) -> dict[str, Any]:
    terminated = next(
        (event for event in events if event["exit_code"] not in {"", "<no value>"}),
        None,
    )
    if terminated is None:
        raise VerificationError("pod termination status was not observed")
    if readiness_false_at >= terminated["observed_at"]:
        raise VerificationError("pod exited before readiness became false")
    try:
        exit_code = int(terminated["exit_code"])
        raw_signal = terminated["signal"]
        signal = 0 if raw_signal in {"", "<no value>"} else int(raw_signal)
    except ValueError as error:
        raise VerificationError(
            "pod returned a non-numeric exit code or signal"
        ) from error
    if exit_code != 0 or signal != 0:
        raise VerificationError(
            f"pod exited with code {exit_code} and signal {signal}; graceful exit was expected"
        )
    elapsed = terminated["observed_at"] - requested_at
    if elapsed >= grace_seconds:
        raise VerificationError(
            f"pod exit took {elapsed:.3f}s, outside its {grace_seconds}s grace period"
        )
    return {
        "readiness_false_before_exit": True,
        "exit_code": exit_code,
        "signal": signal,
        "reason": terminated["reason"],
        "elapsed_seconds": round(elapsed, 3),
        "termination_grace_seconds": grace_seconds,
    }


class DatabaseLock:
    def __init__(self):
        self.process = subprocess.Popen(
            [
                "docker",
                "exec",
                "-i",
                POSTGRES_CONTAINER,
                "psql",
                "-X",
                "-A",
                "-t",
                "-v",
                "ON_ERROR_STOP=1",
                "-U",
                POSTGRES_USER,
                "-d",
                POSTGRES_DATABASE,
            ],
            cwd=ROOT,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        assert self.process.stdin is not None
        self.process.stdin.write(
            "BEGIN;\nLOCK TABLE relay_events IN ACCESS EXCLUSIVE MODE;\n"
            "SELECT 'M4_LOCK_READY';\n"
        )
        self.process.stdin.flush()
        assert self.process.stdout is not None
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            readable, _, _ = select.select([self.process.stdout], [], [], 0.5)
            if not readable:
                if self.process.poll() is not None:
                    break
                continue
            line = self.process.stdout.readline().strip()
            if line == "M4_LOCK_READY":
                return
            if self.process.poll() is not None:
                break
        self.release()
        raise VerificationError("PostgreSQL table lock did not become ready")

    def release(self) -> None:
        if self.process.poll() is not None:
            return
        assert self.process.stdin is not None
        try:
            self.process.stdin.write("ROLLBACK;\n\\q\n")
            self.process.stdin.flush()
            self.process.stdin.close()
            self.process.wait(timeout=5)
        except (BrokenPipeError, subprocess.TimeoutExpired):
            self.process.terminate()
            try:
                self.process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=3)


def blocked_ingest_count() -> int:
    query = (
        "SELECT count(*) FROM pg_stat_activity "
        "WHERE wait_event_type = 'Lock' AND query LIKE '%INSERT INTO relay_events%';"
    )
    output = command(
        [
            "docker",
            "exec",
            POSTGRES_CONTAINER,
            "psql",
            "-X",
            "-A",
            "-t",
            "-U",
            POSTGRES_USER,
            "-d",
            POSTGRES_DATABASE,
            "-c",
            query,
        ]
    ).strip()
    try:
        return int(output)
    except ValueError as error:
        raise VerificationError(f"invalid blocked-ingest count: {output!r}") from error


def wait_until(predicate: Any, description: str, timeout: float) -> Any:
    deadline = time.monotonic() + timeout
    last: Any = None
    while time.monotonic() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(0.1)
    raise VerificationError(
        f"timed out waiting for {description}; last value: {last!r}"
    )


def post_event(url: str, tenant: str, marker: str) -> dict[str, Any]:
    status, payload = http_json(
        "POST",
        f"{url}/v1/events",
        {
            "tenant_id": tenant,
            "type": "k8s.sigterm",
            "data": {"marker": marker},
            "idempotency_key": marker,
        },
        timeout=40,
    )
    if status != 202 or not isinstance(payload.get("id"), str) or not payload["id"]:
        raise VerificationError(f"ingest returned {status}: {payload}")
    return payload


def event_is_published(event_id: str) -> bool:
    query = (
        "SELECT count(*) FROM relay_events WHERE id = '"
        + event_id.replace("'", "''")
        + "' AND published_at IS NOT NULL;"
    )
    output = command(
        [
            "docker",
            "exec",
            POSTGRES_CONTAINER,
            "psql",
            "-X",
            "-A",
            "-t",
            "-U",
            POSTGRES_USER,
            "-d",
            POSTGRES_DATABASE,
            "-c",
            query,
        ]
    ).strip()
    return output == "1"


def deliveries(
    sink_url: str, event_id: str, path: str = "/hooks/ok"
) -> list[dict[str, Any]]:
    status, payload = http_json("GET", f"{sink_url}/received")
    if status != 200 or not isinstance(payload.get("deliveries"), list):
        raise VerificationError(f"sink delivery history returned {status}: {payload}")
    return [
        item
        for item in payload["deliveries"]
        if item.get("webhook_id") == event_id and item.get("path") == path
    ]


def attempt_outcomes(ingest_url: str, event_id: str) -> list[str]:
    status, payload = http_json("GET", f"{ingest_url}/v1/events/{event_id}/attempts")
    if status != 200 or payload.get("event_id") != event_id:
        return []
    attempts = payload.get("attempts")
    if not isinstance(attempts, list):
        return []
    return [item.get("outcome", "") for item in attempts if isinstance(item, dict)]


def delivery_result(outcomes: list[str], healthy_count: int) -> str:
    if "delivered" not in outcomes or "exhausted" not in outcomes:
        raise VerificationError("attempt history lacks delivered or exhausted outcome")
    if healthy_count < 1:
        raise VerificationError("the owned record never reached the healthy subscriber")
    return "completed" if healthy_count == 1 else "redelivered"


def pod_grace(application: str) -> int:
    output = kubectl(
        "get",
        f"deployment/{application}",
        "-o",
        "jsonpath={.spec.template.spec.terminationGracePeriodSeconds}",
    ).strip()
    try:
        value = int(output)
    except ValueError as error:
        raise VerificationError(
            f"invalid {application} termination grace: {output!r}"
        ) from error
    if value <= 0:
        raise VerificationError(f"{application} termination grace must be positive")
    return value


def terminate_and_observe(
    pod: str, readiness_port: int, grace_seconds: int
) -> dict[str, Any]:
    readiness_forward = PortForward(f"pod/{pod}", readiness_port)
    wait_http(f"{readiness_forward.url}/readyz")
    watcher = PodWatch(pod)
    time.sleep(0.25)
    requested = time.monotonic()
    try:
        kubectl("delete", "pod", pod, "--wait=false", timeout=15)
        wait_until(
            lambda: readiness_is_false(readiness_forward.url),
            f"{pod} readiness endpoint to return 503",
            10,
        )
        readiness_false_at = time.monotonic()
        wait_until(
            lambda: any(
                event["exit_code"] not in {"", "<no value>"} for event in watcher.events
            ),
            f"{pod} termination status",
            grace_seconds,
        )
        kubectl("wait", "--for=delete", f"pod/{pod}", f"--timeout={grace_seconds}s")
    finally:
        events = watcher.stop()
        readiness_forward.stop()
    return analyze_termination(events, requested, readiness_false_at, grace_seconds)


def wait_request_blocked(future: Future[dict[str, Any]]) -> None:
    def blocked() -> int:
        if future.done():
            future.result()
            raise VerificationError(
                "ingest request completed before the controlled lock"
            )
        return blocked_ingest_count()

    wait_until(blocked, "the ingest request to block on PostgreSQL", 10)


def current_pause_annotation() -> str | None:
    value = kubectl(
        "get",
        "scaledobject/relay-deliver",
        "-o",
        f'go-template={{{{index .metadata.annotations "{PAUSE_ANNOTATION}"}}}}',
    ).strip()
    return None if value in {"", "<no value>"} else value


def set_pause(value: str | None) -> None:
    if value is None:
        kubectl("annotate", "scaledobject/relay-deliver", f"{PAUSE_ANNOTATION}-")
    else:
        kubectl(
            "annotate",
            "scaledobject/relay-deliver",
            f"{PAUSE_ANNOTATION}={value}",
            "--overwrite",
        )


def verify_environment() -> None:
    context = command(["kubectl", "config", "current-context"]).strip()
    if context != CONTEXT:
        raise VerificationError(
            f"current kubectl context is {context!r}, want {CONTEXT!r}"
        )
    compose = command(
        [
            "docker",
            "compose",
            "-f",
            "local/docker-compose.yml",
            "ps",
            "-q",
            "relay-ingest",
            "relay-deliver",
            "sink",
        ]
    ).strip()
    if compose:
        raise VerificationError("compose relay or sink containers are running")
    command(["docker", "inspect", POSTGRES_CONTAINER])


def run_verification() -> dict[str, Any]:
    verify_environment()
    started_at = utc_now()
    marker = f"k8s-sigterm-{time.time_ns()}"
    source_commit = command(["git", "rev-parse", "HEAD"]).strip()
    worktree_clean = not command(["git", "status", "--porcelain"]).strip()
    result: dict[str, Any] = {
        "schema_version": 1,
        "started_at": started_at,
        "context": CONTEXT,
        "source_commit": source_commit,
        "worktree_clean": worktree_clean,
        "result": "failed",
        "ingest": {},
        "deliver": {},
        "cleanup": {},
    }
    forwards: list[PortForward] = []
    sink_forward: PortForward | None = None
    database_lock: DatabaseLock | None = None
    original_pause = current_pause_annotation()
    executor = ThreadPoolExecutor(max_workers=1)
    try:
        set_pause("1")
        wait_ready_pods("relay-deliver", 1)
        ingest_pods = wait_ready_pods("relay-ingest", 2)
        sink_pod = wait_ready_pods("sink", 1)[0]
        sink_provenance = image_provenance(sink_pod, source_commit)
        result["sink"] = {
            "pod": sink_pod["metadata"]["name"],
            "pod_uid": sink_pod["metadata"]["uid"],
            **sink_provenance,
        }
        ingest_pod = ingest_pods[0]["metadata"]["name"]
        ingest_uid = ingest_pods[0]["metadata"]["uid"]
        ingest_provenance = image_provenance(ingest_pods[0], source_commit)
        ingest_grace = pod_grace("relay-ingest")

        ingest_forward = PortForward(f"pod/{ingest_pod}", 8080)
        forwards.append(ingest_forward)
        wait_http(f"{ingest_forward.url}/readyz")
        database_lock = DatabaseLock()
        future = executor.submit(
            post_event, ingest_forward.url, "globex", marker + "-ingest"
        )
        wait_request_blocked(future)

        watcher = PodWatch(ingest_pod)
        time.sleep(0.25)
        requested = time.monotonic()
        kubectl("delete", "pod", ingest_pod, "--wait=false", timeout=15)
        wait_until(
            lambda: readiness_is_false(ingest_forward.url),
            "relay-ingest readiness endpoint to return 503",
            10,
        )
        readiness_false_at = time.monotonic()
        database_lock.release()
        database_lock = None
        accepted = future.result(timeout=35)
        wait_until(
            lambda: any(
                event["exit_code"] not in {"", "<no value>"} for event in watcher.events
            ),
            "old relay-ingest pod termination status",
            ingest_grace,
        )
        kubectl(
            "wait",
            "--for=delete",
            f"pod/{ingest_pod}",
            f"--timeout={ingest_grace}s",
        )
        ingest_events = watcher.stop()
        ingest_termination = analyze_termination(
            ingest_events, requested, readiness_false_at, ingest_grace
        )
        event_id = accepted["id"]
        wait_until(
            lambda: event_is_published(event_id), "durable ingest publication", 15
        )
        wait_ready_pods("relay-ingest", 2)

        sink_forward = PortForward("service/sink", 8081)
        forwards.append(sink_forward)
        wait_http(f"{sink_forward.url}/healthz")
        if not sink_is_at_baseline(sink_forward.url):
            raise VerificationError(
                "sink control is not at the zero-latency, zero-failure, unlatched baseline"
            )
        wait_until(
            lambda: deliveries(sink_forward.url, event_id),
            "delivery of the in-flight ingest request",
            30,
        )
        result["ingest"] = {
            "pod": ingest_pod,
            "pod_uid": ingest_uid,
            **ingest_provenance,
            "event_id": event_id,
            "request_status": 202,
            "published": True,
            "delivered": True,
            **ingest_termination,
        }

        service_ingest = PortForward("service/relay-ingest", 80)
        forwards.append(service_ingest)
        wait_http(f"{service_ingest.url}/readyz")
        http_json("DELETE", f"{sink_forward.url}/received")
        latch_status, _ = http_json(
            "POST", f"{sink_forward.url}/control", {"latch": "/hooks/flaky"}
        )
        if latch_status != 200:
            raise VerificationError(f"sink latch returned {latch_status}")

        deliver_event = post_event(service_ingest.url, "acme", marker + "-deliver")
        deliver_event_id = deliver_event["id"]
        wait_until(
            lambda: len(deliveries(sink_forward.url, deliver_event_id)) == 1,
            "the healthy subscriber delivery",
            20,
        )

        def held() -> bool:
            payload = sink_control_state(sink_forward.url)
            return payload.get("held", {}).get("/hooks/flaky", 0) >= 1

        wait_until(held, "relay-deliver to own the latched subscriber request", 10)
        deliver_pod = wait_ready_pods("relay-deliver", 1)[0]
        deliver_name = deliver_pod["metadata"]["name"]
        deliver_uid = deliver_pod["metadata"]["uid"]
        deliver_provenance = image_provenance(deliver_pod, source_commit)
        deliver_termination = terminate_and_observe(
            deliver_name, 8080, pod_grace("relay-deliver")
        )
        http_json("POST", f"{sink_forward.url}/control", {"release": "/hooks/flaky"})
        wait_ready_pods("relay-deliver", 1)
        outcomes = wait_until(
            lambda: (
                values
                if "delivered"
                in (values := attempt_outcomes(service_ingest.url, deliver_event_id))
                and "exhausted" in values
                else []
            ),
            "delivered and exhausted attempt history",
            45,
        )
        healthy_count = len(deliveries(sink_forward.url, deliver_event_id))
        at_least_once = delivery_result(outcomes, healthy_count)
        result["deliver"] = {
            "pod": deliver_name,
            "pod_uid": deliver_uid,
            **deliver_provenance,
            "event_id": deliver_event_id,
            "attempt_outcomes": outcomes,
            "healthy_delivery_count": healthy_count,
            "at_least_once_result": at_least_once,
            **deliver_termination,
        }
        result["result"] = "passed"
    except (
        KeyboardInterrupt,
        VerificationError,
        subprocess.SubprocessError,
        OSError,
        ValueError,
    ) as error:
        result["error"] = str(error) or "interrupted by SIGINT"
    finally:
        cleanup_errors: list[str] = []
        if database_lock is not None:
            database_lock.release()
            database_lock = None
        sink_restored = sink_forward is None
        try:
            if sink_forward is not None:
                status, payload = http_json(
                    "POST",
                    f"{sink_forward.url}/control",
                    {
                        "latency_ms": 0,
                        "fail_rate": 0,
                        "release": "/hooks/flaky",
                    },
                )
                if status != 200 or not sink_is_at_baseline(sink_forward.url):
                    cleanup_errors.append(
                        f"sink baseline restore returned {status}: {payload}"
                    )
                else:
                    sink_restored = True
        except (OSError, ValueError, VerificationError) as error:
            cleanup_errors.append(f"sink baseline restore failed: {error}")
        keda_restored = False
        try:
            set_pause(original_pause)
            keda_restored = current_pause_annotation() == original_pause
            if not keda_restored:
                cleanup_errors.append("KEDA pause annotation did not restore exactly")
        except (OSError, ValueError, VerificationError) as error:
            cleanup_errors.append(f"KEDA pause restore failed: {error}")
        for forward in reversed(forwards):
            forward.stop()
        forwards_stopped = all(
            forward.process.poll() is not None for forward in forwards
        )
        if not forwards_stopped:
            cleanup_errors.append("one or more port-forwards remained running")
        executor.shutdown(wait=False, cancel_futures=True)
        result["cleanup"] = {
            "sink_baseline_restored": sink_restored,
            "keda_pause_restored": keda_restored,
            "port_forwards_stopped": forwards_stopped,
            "database_lock_released": database_lock is None,
        }
        if cleanup_errors:
            result["result"] = "failed"
            existing = result.get("error")
            result["error"] = "; ".join(
                [part for part in [existing, *cleanup_errors] if part]
            )
        result["finished_at"] = utc_now()
    if result["result"] != "passed":
        raise VerificationRunError(result)
    return result


def write_receipt(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            json.dump(payload, output, indent=2, sort_keys=True)
            output.write("\n")
        os.replace(temporary, path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def validate_output(path: Path) -> None:
    raw_root = LOCAL_EVIDENCE_ROOT
    absolute = path if path.is_absolute() else ROOT / path
    try:
        absolute.resolve(strict=False).relative_to(raw_root.resolve())
    except ValueError as error:
        raise VerificationError(f"output must be beneath {raw_root}") from error
    current = absolute
    while current != ROOT:
        if current.is_symlink():
            raise VerificationError(f"output path contains a symlink: {current}")
        current = current.parent
    if path.exists():
        raise VerificationError(f"output already exists: {path}")


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--output", type=Path, required=True)
    return result


def main() -> int:
    args = parser().parse_args()
    payload: dict[str, Any]
    output_valid = False

    def terminate(_signum: int, _frame: Any) -> None:
        raise VerificationError("interrupted by SIGTERM")

    signal.signal(signal.SIGTERM, terminate)
    try:
        validate_output(args.output)
        output_valid = True
        payload = run_verification()
    except (
        KeyboardInterrupt,
        VerificationError,
        subprocess.SubprocessError,
        OSError,
        ValueError,
    ) as error:
        detail = str(error) or "interrupted by SIGINT"
        payload = getattr(
            error,
            "receipt",
            {
                "schema_version": 1,
                "finished_at": utc_now(),
                "context": CONTEXT,
                "result": "failed",
                "error": detail,
            },
        )
        if output_valid:
            write_receipt(args.output, payload)
        print(f"k8s SIGTERM verification: {detail}", file=sys.stderr)
        return 1
    write_receipt(args.output, payload)
    if payload.get("result") != "passed":
        print(
            f"k8s SIGTERM verification: {payload.get('error', 'failed')}",
            file=sys.stderr,
        )
        return 1
    print(f"k8s SIGTERM verification passed: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
