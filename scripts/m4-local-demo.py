#!/usr/bin/env python3
"""Run and record the machine-checked M4 demo against local minikube."""

from __future__ import annotations

import argparse
import datetime
import importlib.util
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile
import time
from types import ModuleType
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
LOCAL_EVIDENCE_ROOT = ROOT / ".evidence" / "m4-local"
RUN_ID_RE = re.compile(r"^[0-9]{8}T[0-9]{6}Z$")


class DemoError(RuntimeError):
    """The integrated local demo did not prove its contract."""


def utc_now() -> str:
    return (
        datetime.datetime.now(datetime.UTC)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def load_script_module(name: str, filename: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / filename)
    if spec is None or spec.loader is None:
        raise DemoError(f"cannot load scripts/{filename}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def validate_output(path: Path) -> Path:
    absolute = path if path.is_absolute() else ROOT / path
    try:
        absolute.resolve(strict=False).relative_to(LOCAL_EVIDENCE_ROOT.resolve())
    except ValueError as error:
        raise DemoError(f"output must be beneath {LOCAL_EVIDENCE_ROOT}") from error
    current = absolute
    while current != ROOT:
        if current.is_symlink():
            raise DemoError(f"output path contains a symlink: {current}")
        current = current.parent
    if absolute.exists():
        raise DemoError(f"output already exists: {absolute}")
    if not RUN_ID_RE.fullmatch(absolute.parent.name):
        raise DemoError("output parent must be a UTC YYYYMMDDTHHMMSSZ run id")
    absolute.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(absolute.parent, 0o700)
    return absolute


def write_file(path: Path, content: str, mode: int = 0o600) -> None:
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, mode)
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            output.write(content)
        os.replace(temporary, path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def command(arguments: list[str]) -> str:
    result = subprocess.run(
        arguments,
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
        timeout=30,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise DemoError(f"{' '.join(arguments)} failed: {detail}")
    return result.stdout


def collect_provenance(sigterm: ModuleType, source_commit: str) -> dict[str, Any]:
    expected = {"relay-ingest": 2, "sink": 1}
    result: dict[str, Any] = {}
    for application, count in expected.items():
        pods = sigterm.ready_pods(application)
        if len(pods) != count:
            raise DemoError(f"{application} has {len(pods)} ready pods, want {count}")
        result[application] = [
            {
                "pod": pod["metadata"]["name"],
                "pod_uid": pod["metadata"]["uid"],
                **sigterm.image_provenance(pod, source_commit),
            }
            for pod in pods
        ]
    deliver = sigterm.ready_pods("relay-deliver")
    if not deliver:
        raise DemoError("relay-deliver has no ready pod")
    result["relay-deliver"] = [
        {
            "pod": pod["metadata"]["name"],
            "pod_uid": pod["metadata"]["uid"],
            **sigterm.image_provenance(pod, source_commit),
        }
        for pod in deliver
    ]
    return result


def run_streamed(arguments: list[str]) -> tuple[int, str]:
    process = subprocess.Popen(
        arguments,
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        start_new_session=True,
    )

    def interrupt(_signum: int, _frame: Any) -> None:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)

    previous_int = signal.signal(signal.SIGINT, interrupt)
    previous_term = signal.signal(signal.SIGTERM, interrupt)
    lines: list[str] = []
    try:
        assert process.stdout is not None
        for line in process.stdout:
            print(line, end="", flush=True)
            lines.append(line)
        return process.wait(), "".join(lines)
    finally:
        signal.signal(signal.SIGINT, previous_int)
        signal.signal(signal.SIGTERM, previous_term)
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=5)
        if process.stdout is not None:
            process.stdout.close()


def summarize_demo(transcript: str) -> dict[str, Any]:
    scale_samples = [
        (int(lag), int(consumers))
        for lag, consumers in re.findall(
            r"t=\d+s\s+(\d+)\s+(\d+)", transcript
        )
    ]
    outcomes = [
        (int(delivered), int(dead_lettered))
        for delivered, dead_lettered in re.findall(
            r"delivered=(\d+)\s+dead-lettered=(\d+)", transcript
        )
    ]
    event_ids = re.findall(r'\{"id":"(evt_[0-9a-f]{32})"\}', transcript)
    replay_completed = (
        "replaying. every event after that point is being delivered again" in transcript
    )
    if not scale_samples or scale_samples[-1] != (0, 1):
        raise DemoError("demo transcript has no final zero-lag, one-consumer sample")
    if len(outcomes) < 2 or outcomes[-1][0] <= outcomes[0][0]:
        raise DemoError("demo transcript has no fresh healthy delivery")
    if outcomes[-1][1] <= outcomes[0][1]:
        raise DemoError("demo transcript has no fresh dead-letter outcome")
    if len(event_ids) != 2:
        raise DemoError(f"demo transcript has {len(event_ids)} event ids, want 2")
    if not replay_completed:
        raise DemoError("demo transcript has no completed replay")
    return {
        "event_ids": event_ids,
        "scale_samples": len(scale_samples),
        "peak_lag": max(lag for lag, _ in scale_samples),
        "peak_consumers": max(consumers for _, consumers in scale_samples),
        "final_lag": scale_samples[-1][0],
        "final_consumers": scale_samples[-1][1],
        "healthy_delivery_counter_delta": outcomes[-1][0] - outcomes[0][0],
        "dead_letter_counter_delta": outcomes[-1][1] - outcomes[0][1],
        "replay_completed": replay_completed,
    }


def verify_demo_cleanup(sigterm: ModuleType) -> dict[str, bool]:
    if sigterm.current_pause_annotation() is not None:
        raise DemoError("relay demo left KEDA paused")
    forward = sigterm.PortForward("service/sink", 8081)
    try:
        sigterm.wait_http(f"{forward.url}/healthz")
        if not sigterm.sink_is_at_baseline(forward.url):
            raise DemoError("relay demo left sink controls changed")
    finally:
        forward.stop()
    return {
        "keda_pause_absent": True,
        "sink_baseline_restored": True,
        "verification_port_forward_stopped": forward.process.poll() is not None,
    }


def rehearse(output: Path) -> dict[str, Any]:
    destination = validate_output(output)
    transcript = destination.with_name("demo-transcript.txt")
    if transcript.exists():
        raise DemoError(f"transcript already exists: {transcript}")
    sigterm = load_script_module("m4_demo_sigterm", "verify-k8s-sigterm.py")
    evidence = load_script_module("m4_demo_evidence", "m4-evidence.py")
    source_commit = command(["git", "rev-parse", "HEAD"]).strip()
    worktree_clean = not command(["git", "status", "--porcelain"]).strip()
    try:
        sigterm.verify_environment()
        if sigterm.current_pause_annotation() is not None:
            raise DemoError("KEDA pause annotation must be absent before the demo")
        provenance = collect_provenance(sigterm, source_commit)
    except RuntimeError as error:
        raise DemoError(str(error)) from error
    required_visuals = [
        item["file"] for item in evidence.screenshot_steps() if item.get("required")
    ]
    started_at = utc_now()
    started = time.monotonic()
    transcript_parts: list[str] = []
    result = "failed"
    error = ""
    cleanup: dict[str, bool] = {}
    observations: dict[str, Any] = {}
    try:
        for arguments in (["make", "monitoring-ready"], ["make", "relay-demo"]):
            transcript_parts.append(f"$ {' '.join(arguments)}\n")
            return_code, text = run_streamed(arguments)
            transcript_parts.append(text)
            if return_code != 0:
                raise DemoError(
                    f"{' '.join(arguments)} exited with status {return_code}"
                )
        observations = summarize_demo("".join(transcript_parts))
        cleanup = verify_demo_cleanup(sigterm)
        result = "passed"
    except (DemoError, OSError, subprocess.SubprocessError) as caught:
        error = str(caught)
        try:
            cleanup = verify_demo_cleanup(sigterm)
        except (DemoError, OSError, subprocess.SubprocessError) as cleanup_error:
            cleanup = {"verified": False}
            error = f"{error}; cleanup verification failed: {cleanup_error}"
    finished_at = utc_now()
    write_file(transcript, "".join(transcript_parts))
    payload: dict[str, Any] = {
        "schema_version": 1,
        "started_at": started_at,
        "finished_at": finished_at,
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "result": result,
        "run_id": destination.parent.name,
        "source_commit": source_commit,
        "worktree_clean": worktree_clean,
        "commands": ["make monitoring-ready", "make relay-demo"],
        "provenance": provenance,
        "observations": observations,
        "cleanup": cleanup,
        "transcript": transcript.name,
        "required_visual_order": required_visuals,
        "visual_capture_status": "pending-human-capture",
    }
    if error:
        payload["error"] = error
    write_file(destination, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    if result != "passed":
        raise DemoError(error)
    return payload


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--output", type=Path, required=True)
    return result


def main() -> int:
    args = parser().parse_args()
    try:
        receipt = rehearse(args.output)
    except (DemoError, OSError, subprocess.SubprocessError) as error:
        print(f"M4 local demo rehearsal: {error}", file=sys.stderr)
        return 1
    print(
        f"M4 local demo rehearsal passed in {receipt['elapsed_seconds']}s: {args.output}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
