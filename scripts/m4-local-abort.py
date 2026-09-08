#!/usr/bin/env python3
"""Rehearse the M4 interruption and destroy-first path without AWS."""

from __future__ import annotations

import argparse
import datetime
import importlib.util
import json
import os
from pathlib import Path
import secrets
import select
import shutil
import signal
import subprocess
import sys
import tempfile
from types import ModuleType
from typing import Any, Callable


ROOT = Path(__file__).resolve().parent.parent
LOCAL_EVIDENCE_ROOT = ROOT / ".evidence" / "m4-local"
SCRIPT = Path(__file__).resolve()
EXPECTED_ABORT_OUTPUTS = (
    "20-destroy.txt",
    "21-inventory-after.json",
    "22-cost-immediate.txt",
)
EXPECTED_WORKER_EVENTS = (
    "deployment_started",
    "sigterm_received",
    "destroy_first",
    "after_destroy_inventory",
    "after_destroy_cost",
    "temporary_credentials_removed",
)


class RehearsalError(RuntimeError):
    """The local interruption rehearsal did not prove its contract."""


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
        raise RehearsalError(f"cannot load scripts/{filename}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


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
        raise RehearsalError(f"{' '.join(arguments)} failed: {detail}")
    return result.stdout


def validate_output(path: Path) -> Path:
    absolute = path if path.is_absolute() else ROOT / path
    try:
        absolute.resolve(strict=False).relative_to(LOCAL_EVIDENCE_ROOT.resolve())
    except ValueError as error:
        raise RehearsalError(f"output must be beneath {LOCAL_EVIDENCE_ROOT}") from error
    current = absolute
    while current != ROOT:
        if current.is_symlink():
            raise RehearsalError(f"output path contains a symlink: {current}")
        current = current.parent
    if absolute.exists():
        raise RehearsalError(f"output already exists: {absolute}")
    absolute.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(absolute.parent, 0o700)
    return absolute


def write_json(path: Path, payload: dict[str, Any]) -> None:
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


def validate_abort_protocol(
    make_dry_run: Callable[[list[str]], str] = command,
) -> dict[str, Any]:
    preflight = load_script_module("m4_preflight_abort", "m4-preflight.py")
    evidence = load_script_module("m4_evidence_abort", "m4-evidence.py")
    protocol = evidence.validate_protocol()
    captures = evidence.capture_steps()
    abort = [
        item for item in captures if item["phase"] in {"destroy_first", "after_destroy"}
    ]
    outputs = tuple(item["output"] for item in abort)
    if outputs != EXPECTED_ABORT_OUTPUTS:
        raise RehearsalError(
            f"abort outputs are {outputs!r}, want {EXPECTED_ABORT_OUTPUTS!r}"
        )
    if not abort[0]["command"].startswith("make aws-down"):
        raise RehearsalError("abort does not start with make aws-down")
    if "inventory" not in abort[1]["command"]:
        raise RehearsalError("destroy is not followed by the resource inventory")
    dry_run = make_dry_run(["make", "--dry-run", "aws-down"])
    preflight.validate_destroy_recipe(dry_run)
    live_outputs = [
        item["output"] for item in captures if item["phase"] == "live_window"
    ]
    screenshots: list[str] = []
    for item in evidence.screenshot_steps():
        if item.get("required"):
            screenshots.append(item["file"])
        else:
            screenshots.extend(item["files"])
    return {
        "capture_count": protocol["capture_count"],
        "abort_outputs": list(outputs),
        "skipped_live_outputs": live_outputs,
        "skipped_screenshots": screenshots,
        "destroy_recipe": "make --dry-run aws-down",
    }


def worker(state: Path) -> int:
    state.mkdir(parents=True, exist_ok=False, mode=0o700)
    credentials = state / "temporary-credentials"
    credentials.mkdir(mode=0o700)
    canary = secrets.token_urlsafe(32)
    credential_file = credentials / "credentials.json"
    descriptor = os.open(credential_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as output:
        json.dump({"access_key": canary, "session_token": canary}, output)
    resource = state / "simulated-hourly-resources.json"
    resource.write_text(
        json.dumps({"eks": 1, "msk": 1, "rds": 1}) + "\n", encoding="utf-8"
    )
    events = ["deployment_started"]
    interrupted = False

    def request_abort(_signum: int, _frame: Any) -> None:
        nonlocal interrupted
        interrupted = True

    signal.signal(signal.SIGTERM, request_abort)
    print("READY", flush=True)
    while not interrupted:
        signal.pause()

    events.append("sigterm_received")
    events.append("destroy_first")
    resource.unlink()
    events.append("after_destroy_inventory")
    audit_empty = not resource.exists()
    events.append("after_destroy_cost")
    credential_file.unlink()
    credentials.rmdir()
    events.append("temporary_credentials_removed")
    result = {
        "events": events,
        "resource_audit_empty": audit_empty,
        "temporary_credentials_removed": not credentials.exists(),
        "credential_canary_absent": True,
    }
    if canary in json.dumps(result, sort_keys=True):
        raise RehearsalError("credential canary escaped into the worker result")
    write_json(state / "worker-result.json", result)
    return 0


def run_worker(workspace: Path) -> dict[str, Any]:
    state = workspace / "worker-state"
    process = subprocess.Popen(
        [sys.executable, str(SCRIPT), "_worker", "--state", str(state)],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )
    try:
        assert process.stdout is not None
        readable, _, _ = select.select([process.stdout], [], [], 10)
        if not readable or process.stdout.readline().strip() != "READY":
            raise RehearsalError("abort worker did not reach simulated deployment")
        process.terminate()
        try:
            return_code = process.wait(timeout=10)
        except subprocess.TimeoutExpired as error:
            raise RehearsalError("abort worker did not finish after SIGTERM") from error
        if return_code != 0:
            assert process.stderr is not None
            raise RehearsalError(
                f"abort worker exited {return_code}: {process.stderr.read().strip()}"
            )
        result_path = state / "worker-result.json"
        try:
            result = json.loads(result_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise RehearsalError("abort worker returned no valid result") from error
        return result
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)
        if process.stdout is not None:
            process.stdout.close()
        if process.stderr is not None:
            process.stderr.close()


def validate_worker_result(result: dict[str, Any]) -> None:
    if tuple(result.get("events", [])) != EXPECTED_WORKER_EVENTS:
        raise RehearsalError("interruption did not run the destroy-first sequence")
    if result.get("resource_audit_empty") is not True:
        raise RehearsalError("post-destroy resource inventory was not empty")
    if result.get("temporary_credentials_removed") is not True:
        raise RehearsalError("temporary credentials survived interruption cleanup")
    if result.get("credential_canary_absent") is not True:
        raise RehearsalError("worker did not prove credential canary removal")


def rehearse(output: Path) -> dict[str, Any]:
    destination = validate_output(output)
    started_at = utc_now()
    protocol = validate_abort_protocol()
    workspace = Path(
        tempfile.mkdtemp(prefix=".abort-rehearsal.", dir=destination.parent)
    )
    os.chmod(workspace, 0o700)
    try:
        worker_result = run_worker(workspace)
        validate_worker_result(worker_result)
    finally:
        shutil.rmtree(workspace, ignore_errors=True)
    if workspace.exists():
        raise RehearsalError("temporary rehearsal workspace survived cleanup")
    payload = {
        "schema_version": 1,
        "started_at": started_at,
        "finished_at": utc_now(),
        "result": "passed",
        "source_commit": command(["git", "rev-parse", "HEAD"]).strip(),
        "worktree_clean": not command(["git", "status", "--porcelain"]).strip(),
        "signal": "SIGTERM",
        "events": worker_result["events"],
        "resource_audit_empty": True,
        "temporary_credentials_removed": True,
        "temporary_workspace_removed": True,
        "credential_canary_absent": True,
        **protocol,
    }
    serialized = json.dumps(payload, sort_keys=True)
    if "access_key" in serialized or "session_token" in serialized:
        raise RehearsalError("credential field escaped into the receipt")
    write_json(destination, payload)
    return payload


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    subparsers = result.add_subparsers(dest="command", required=True)
    run = subparsers.add_parser("rehearse")
    run.add_argument("--output", type=Path, required=True)
    internal = subparsers.add_parser("_worker", help=argparse.SUPPRESS)
    internal.add_argument("--state", type=Path, required=True)
    return result


def main() -> int:
    args = parser().parse_args()
    try:
        if args.command == "_worker":
            return worker(args.state)
        payload = rehearse(args.output)
    except (RehearsalError, OSError, subprocess.SubprocessError) as error:
        print(f"M4 local abort rehearsal: {error}", file=sys.stderr)
        return 1
    print(
        "M4 local abort rehearsal passed: "
        + str(args.output)
        + f" ({len(payload['events'])} ordered events)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
