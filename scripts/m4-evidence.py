#!/usr/bin/env python3
"""Plan, sanitize, and verify the M4 evidence packet."""

from __future__ import annotations

import argparse
import base64
import binascii
import datetime
from decimal import Decimal, InvalidOperation
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import shutil
import struct
import subprocess
import sys
import tempfile
from typing import Any, Mapping
import zlib


ROOT = Path(__file__).resolve().parent.parent
RUN_ID_RE = re.compile(r"^[0-9]{8}T[0-9]{6}Z$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
REDACTION_NAME_RE = re.compile(r"^[A-Z][A-Z0-9_]{1,63}$")
REQUIRED_REDACTION_NAMES = {
    "ACCOUNT_ID",
    "OPERATOR",
}
SECRET_NAMES = {"DATABASE_PASSWORD", "SIGNING_SECRET", "DATABASE_URL"}
SECRET_REPRESENTATIONS = {
    "raw",
    "json-go",
    "json-ascii",
    "userinfo",
    "url-query",
    "base64",
    "base64url",
}
UTC_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
APPLY_START_MAX_AGE = datetime.timedelta(minutes=15)

PROVISIONAL_TEXT = (
    "00-session.json",
    "01-identity.txt",
    "02-prices.md",
    "03-plan-summary.json",
    "04-inventory-before.json",
    "05-images.json",
    "06-go-no-go.json",
    "10-event.json",
    "11-attempts.json",
    "12-metrics.txt",
    "13-trace.json",
    "14-keda.txt",
    "15-dlq.json",
    "16-replay.json",
    "20-destroy.txt",
    "21-inventory-after.json",
    "22-cost-immediate.txt",
)
FINAL_TEXT = PROVISIONAL_TEXT + ("23-cost-final.txt",)
CAPTURE_FILES = PROVISIONAL_TEXT[7:14] + ("application-logs.txt",)
REQUIRED_SCREENSHOTS = (
    "argocd-apps.png",
    "terminal-demo.png",
    "grafana-lag.png",
    "tempo-trace.png",
)
OPTIONAL_SCREENSHOTS = (
    "aws-console-eks.png",
    "aws-console-msk.png",
    "aws-console-rds.png",
    "aws-console-cost.png",
)

ARN_ACCOUNT_RE = re.compile(
    r"\barn:(?:aws|aws-us-gov|aws-cn):[a-z0-9-]*:[a-z0-9-]*:\d{12}:",
    re.IGNORECASE,
)
ECR_RE = re.compile(
    r"\b\d{12}\.dkr\.ecr\.[a-z0-9-]+\.amazonaws\.com(?:\.cn)?\b",
    re.IGNORECASE,
)
RDS_RE = re.compile(
    r"\b[a-z0-9-]+\.[a-z0-9-]+\.[a-z0-9-]+\.rds\.amazonaws\.com(?:\.cn)?\b",
    re.IGNORECASE,
)
MSK_RE = re.compile(
    r"\b(?:[a-z0-9-]+\.)+(?:kafka|kafka-serverless)\.[a-z0-9-]+"
    r"\.amazonaws\.com(?:\.cn)?\b",
    re.IGNORECASE,
)
EMAIL_RE = re.compile(r"(?<![\w.+-])[\w.+-]+@[a-z0-9.-]+\.[a-z]{2,}(?![\w.-])", re.I)
IPV4_RE = re.compile(r"(?<![\d.])(?:\d{1,3}\.){3}\d{1,3}(?![\d.])")
PRESERVED_IDENTIFIER_RE = re.compile(
    r"sha256:[0-9a-f]{64}"
    r"|(?<![0-9a-f])[0-9a-f]{40}(?![0-9a-f])"
    r"|(?<![0-9a-f])[0-9a-f]{32}(?![0-9a-f])"
    r"|\bevt_[A-Za-z0-9_-]+\b"
    r"|\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z",
    re.IGNORECASE,
)


class EvidenceError(RuntimeError):
    """The evidence packet does not satisfy its publication contract."""


def validate_run_id(run_id: str) -> None:
    if not RUN_ID_RE.fullmatch(run_id):
        raise EvidenceError("AWS_RUN_ID must use UTC YYYYMMDDTHHMMSSZ")
    try:
        parsed = datetime.datetime.strptime(run_id, "%Y%m%dT%H%M%SZ").replace(
            tzinfo=datetime.UTC
        )
    except ValueError as error:
        raise EvidenceError("AWS_RUN_ID is not a real UTC date and time") from error
    if parsed.strftime("%Y%m%dT%H%M%SZ") != run_id:
        raise EvidenceError("AWS_RUN_ID is not a canonical UTC timestamp")


def ensure_beneath(path: Path, root: Path) -> None:
    resolved_root = root.resolve()
    try:
        path.resolve(strict=False).relative_to(resolved_root)
    except ValueError as error:
        raise EvidenceError(f"evidence path escapes the repository: {path}") from error

    current = path
    while current != root:
        if current.is_symlink():
            raise EvidenceError(f"evidence path contains a symlink: {current}")
        if root not in current.parents:
            raise EvidenceError(f"evidence path escapes the repository: {path}")
        current = current.parent


def evidence_paths(root: Path, run_id: str) -> tuple[Path, Path]:
    validate_run_id(run_id)
    raw = root / ".evidence" / "m4" / run_id
    published = root / "docs" / "evidence" / "m4" / run_id
    ensure_beneath(raw, root)
    ensure_beneath(published, root)
    return raw, published


def unique_json_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise EvidenceError("duplicate JSON object key")
        result[key] = value
    return result


def read_json_object(path: Path, description: str) -> dict[str, Any]:
    try:
        value = json.loads(
            path.read_text(encoding="utf-8"), object_pairs_hook=unique_json_object
        )
    except FileNotFoundError as error:
        raise EvidenceError(f"{description} is missing: {path}") from error
    except json.JSONDecodeError as error:
        raise EvidenceError(f"{description} is invalid JSON: {error}") from error
    if not isinstance(value, dict):
        raise EvidenceError(f"{description} must be a JSON object")
    return value


def write_json_exclusive(path: Path, payload: Mapping[str, Any], mode: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if mode == 0o600:
        path.parent.chmod(0o700)
    temporary = path.with_name(f".{path.name}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            json.dump(payload, output, indent=2, sort_keys=True)
            output.write("\n")
        os.link(
            temporary, path
        )  # Atomic installation which cannot overwrite a receipt.
        temporary.unlink()
        path.chmod(mode)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def write_json_atomic(path: Path, payload: Mapping[str, Any], mode: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if mode == 0o600:
        path.parent.chmod(0o700)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, mode)
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            json.dump(payload, output, indent=2, sort_keys=True)
            output.write("\n")
        os.replace(temporary, path)
        path.chmod(mode)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def capture_steps() -> list[dict[str, Any]]:
    """Return the fixed export order used by staging and the live run."""
    steps = [
        {
            "order": 1,
            "phase": "before_paid_window",
            "output": "01-identity.txt",
            "source": "repository identity plus read-only AWS account APIs",
            "command": "make aws-account-check AWS_RUN_ID=$AWS_RUN_ID AWS_APPROVED_COMMIT=$AWS_APPROVED_COMMIT",
            "implemented_by": "#96",
        },
        {
            "order": 2,
            "phase": "before_paid_window",
            "output": "02-prices.md",
            "source": "dated official AWS price sources and checked arithmetic",
            "command": "make aws-prices AWS_RUN_ID=$AWS_RUN_ID AWS_APPROVED_COMMIT=$AWS_APPROVED_COMMIT",
            "implemented_by": "#96",
        },
        {
            "order": 3,
            "phase": "before_paid_window",
            "output": "03-plan-summary.json",
            "source": "reviewed Terraform plan",
            "command": 'make aws-plan AWS_RUN_ID=$AWS_RUN_ID AWS_APPROVED_COMMIT=$AWS_APPROVED_COMMIT AWS_PLAN_SUMMARY=$RAW_EVIDENCE/03-plan-summary.json AWS_TF_ARGS="-var enable_eks=true -var enable_msk=true -var enable_rds=true -var eks_operator_cidr=${AWS_OPERATOR_CIDR:?set the reviewed IPv4 /32}"',
            "precondition": "cheap tier applied, inventory and images staged; AWS_OPERATOR_CIDR is the current reviewed IPv4 /32; plan only, no apply",
        },
        {
            "order": 4,
            "phase": "before_paid_window",
            "output": "04-inventory-before.json",
            "source": "tagged and service-native AWS inventory helper owned by #96",
            "command": "make aws-inventory-empty AWS_RUN_ID=$AWS_RUN_ID AWS_APPROVED_COMMIT=$AWS_APPROVED_COMMIT AWS_INVENTORY_FILE=$RAW_EVIDENCE/04-inventory-before.json",
            "implemented_by": "#96",
        },
        {
            "order": 5,
            "phase": "before_paid_window",
            "output": "05-images.json",
            "source": "ECR image staging and inspection helper owned by #96",
            "command": "make aws-stage-images AWS_RUN_ID=$AWS_RUN_ID AWS_APPROVED_COMMIT=$AWS_APPROVED_COMMIT AWS_IMAGE_EVIDENCE=$RAW_EVIDENCE/05-images.json",
            "implemented_by": "#96",
        },
        {
            "order": 6,
            "phase": "before_paid_window",
            "output": "06-go-no-go.json",
            "source": "cross-checked release packet bound to one run and commit",
            "command": "make aws-go-no-go AWS_RUN_ID=$AWS_RUN_ID AWS_APPROVED_COMMIT=$AWS_APPROVED_COMMIT M4_OPERATOR=$M4_OPERATOR",
            "implemented_by": "#96",
        },
        {
            "order": 7,
            "phase": "before_paid_window",
            "output": "00-session.json",
            "source": "session controller",
            "command": "make aws-live-run AWS_RUN_ID=$AWS_RUN_ID AWS_APPROVED_COMMIT=$AWS_APPROVED_COMMIT M4_OPERATOR=$M4_OPERATOR",
        },
        {
            "order": 10,
            "phase": "live_window",
            "output": "10-event.json",
            "source": "relay ingest response and idempotent repeat",
            "command": "python3 scripts/m4-live-capture.py event --output $RAW_EVIDENCE/10-event.json",
            "implemented_by": "#97",
        },
        {
            "order": 11,
            "phase": "live_window",
            "output": "11-attempts.json",
            "source": "GET /v1/events/{id}/attempts",
            "command": "curl -fsS $RELAY_INGEST_URL/v1/events/$EVENT_ID/attempts | jq . >$RAW_EVIDENCE/11-attempts.json",
        },
        {
            "order": 12,
            "phase": "live_window",
            "output": "12-metrics.txt",
            "source": "fresh Prometheus instant samples during the bounded cohort; source timestamps checked before aggregation",
            "command": "python3 scripts/m4-live-capture.py metrics --prometheus-url $PROMETHEUS_URL --output $RAW_EVIDENCE/12-metrics.txt",
            "implemented_by": "#97",
            "queries": [
                'max(relay_consumer_group_lag_total{group="relay-deliver"})',
                'max(relay_group_members{group="relay-deliver"})',
                'max(relay_topic_partitions_unassigned{group="relay-deliver"})',
                'max(relay_group_unassigned_members{group="relay-deliver"})',
                'kube_deployment_spec_replicas{namespace="mlp",deployment="relay-deliver"}',
            ],
        },
        {
            "order": 13,
            "phase": "live_window",
            "output": "13-trace.json",
            "source": "Tempo trace API joined to the capture event and persisted attempt coordinates",
            "command": "curl -fsS $TEMPO_URL/api/traces/$TRACE_ID | jq . >$RAW_EVIDENCE/13-trace.json",
        },
        {
            "order": 14,
            "phase": "live_window",
            "output": "14-keda.txt",
            "source": "Kubernetes Deployment, pod, and ScaledObject state",
            "command": "kubectl -n mlp get deployment/relay-deliver scaledobject/relay-deliver pods -l app.kubernetes.io/name=relay-deliver -o wide >$RAW_EVIDENCE/14-keda.txt",
        },
        {
            "order": 15,
            "phase": "live_window",
            "output": "15-dlq.json",
            "source": "delivery DLQ consumer with source topic, partition, and offset",
            "command": "python3 scripts/m4-live-capture.py dlq --output $RAW_EVIDENCE/15-dlq.json",
            "implemented_by": "#97",
        },
        {
            "order": 16,
            "phase": "live_window",
            "output": "16-replay.json",
            "source": "relay replay Job logs and matched delivery outcomes",
            "command": "python3 scripts/m4-live-capture.py replay --output $RAW_EVIDENCE/16-replay.json",
            "implemented_by": "#97",
        },
        {
            "order": 17,
            "phase": "live_window",
            "output": "application-logs.txt",
            "source": "all relay and sink containers since the session start",
            "command": "kubectl -n mlp logs -l app.kubernetes.io/part-of=relay --all-containers --prefix --since-time=$BILLABLE_STARTED_AT >$RAW_EVIDENCE/application-logs.txt",
            "publication": "scan-only; excerpts belong in the named JSON evidence",
        },
        {
            "order": 20,
            "phase": "destroy_first",
            "output": "20-destroy.txt",
            "source": "controller-owned state-backed destroy, log cleanup, and empty-state transcript",
            "command": "make aws-down; make aws-state-empty",
        },
        {
            "order": 21,
            "phase": "after_destroy",
            "output": "21-inventory-after.json",
            "source": "same inventory helper and query set as 04-inventory-before.json",
            "command": "make aws-inventory-empty AWS_RUN_ID=$AWS_RUN_ID AWS_APPROVED_COMMIT=$AWS_APPROVED_COMMIT AWS_INVENTORY_FILE=$RAW_EVIDENCE/21-inventory-after.json",
            "implemented_by": "#96",
        },
        {
            "order": 22,
            "phase": "after_destroy",
            "output": "22-cost-immediate.txt",
            "source": "provisional Cost Explorer output",
            "command": "make aws-cost >$RAW_EVIDENCE/22-cost-immediate.txt",
        },
        {
            "order": 23,
            "phase": "after_billing_settles",
            "output": "23-cost-final.txt",
            "source": "settled account-wide daily service totals over session UTC dates; not exact M4 attribution",
            "command": "make aws-cost-final AWS_RUN_ID=$AWS_RUN_ID",
        },
    ]
    command = (
        "python3 scripts/m4-live-capture.py --environment aws "
        '--context "$AWS_KUBE_CONTEXT" --run-id "$AWS_RUN_ID" '
        '--commit "$AWS_APPROVED_COMMIT"'
    )
    for step in steps:
        if step["phase"] == "live_window":
            step["command"] = command + (
                " --deploy"
                if step["order"] == 10
                else " --verify-output " + step["output"]
            )
            step["execution"] = (
                "one bounded run at step 10; later commands verify its exports without repeating the load"
            )
    return steps


def screenshot_steps() -> list[dict[str, Any]]:
    return [
        {
            "order": 1,
            "file": "argocd-apps.png",
            "surface": "ArgoCD",
            "visible_state": "all child Applications are synced and healthy",
            "required": True,
        },
        {
            "order": 2,
            "file": "terminal-demo.png",
            "surface": "terminal",
            "visible_state": "event, idempotency, attempt, DLQ, and replay assertions passed",
            "required": True,
        },
        {
            "order": 3,
            "file": "grafana-lag.png",
            "surface": "Grafana relay dashboard",
            "visible_state": "lag rise and drain plus scale out and return to one",
            "required": True,
        },
        {
            "order": 4,
            "file": "tempo-trace.png",
            "surface": "Grafana Explore with Tempo",
            "visible_state": "one trace contains ingest, produce, consume, and webhook attempts",
            "required": True,
        },
        {
            "order": 5,
            "files": list(OPTIONAL_SCREENSHOTS),
            "surface": "AWS console",
            "visible_state": "optional orientation views; command exports remain authoritative",
            "required": False,
            "deadline_behavior": "skip rather than delay destroy",
        },
    ]


def validate_protocol() -> dict[str, Any]:
    captures = capture_steps()
    capture_orders = [item["order"] for item in captures]
    if capture_orders != sorted(capture_orders) or len(capture_orders) != len(
        set(capture_orders)
    ):
        raise EvidenceError("capture order must be sorted and unique")
    capture_outputs = {
        item["output"] for item in captures if item.get("publication") is None
    }
    missing_outputs = sorted(set(FINAL_TEXT) - capture_outputs)
    if missing_outputs:
        raise EvidenceError("capture plan omits: " + ", ".join(missing_outputs))
    for item in captures:
        command = item.get("command")
        if not isinstance(command, str) or not command.strip():
            raise EvidenceError(f"capture {item.get('output')} has no command")
        if "docs/evidence/m4" in command:
            raise EvidenceError(
                f"capture {item['output']} writes to the publication tree"
            )

    phases = [item["phase"] for item in captures]
    destroy_index = phases.index("destroy_first")
    if any(phase == "live_window" for phase in phases[destroy_index + 1 :]):
        raise EvidenceError("live capture cannot follow destroy")
    if any(phase == "after_destroy" for phase in phases[:destroy_index]):
        raise EvidenceError("post-destroy capture appears before destroy")

    screenshots = screenshot_steps()
    required = tuple(item["file"] for item in screenshots if item.get("required"))
    if required != REQUIRED_SCREENSHOTS:
        raise EvidenceError("required screenshot order changed")
    optional = screenshots[-1]
    if (
        optional.get("files") != list(OPTIONAL_SCREENSHOTS)
        or optional.get("deadline_behavior") != "skip rather than delay destroy"
    ):
        raise EvidenceError(
            "optional AWS screenshots must yield to the destroy deadline"
        )
    return {
        "capture_count": len(captures),
        "required_screenshots": list(REQUIRED_SCREENSHOTS),
        "optional_screenshots": list(OPTIONAL_SCREENSHOTS),
        "provisional_file_count": len(PROVISIONAL_TEXT),
        "final_file_count": len(FINAL_TEXT),
    }


def validate_preflight(raw: Path, run_id: str) -> dict[str, Any]:
    path = raw / "00-preflight.json"
    ensure_beneath(path, raw.parents[2])
    if path.is_symlink() or not path.is_file():
        raise EvidenceError(f"passing preflight receipt is missing: {path}")
    receipt = read_json_object(path, "preflight receipt")
    commit = receipt.get("commit")
    if (
        receipt.get("schema_version") != 1
        or receipt.get("run_id") != run_id
        or receipt.get("result") != "passed"
        or not isinstance(commit, str)
        or not COMMIT_RE.fullmatch(commit)
    ):
        raise EvidenceError("preflight receipt is not a pass for this run and commit")
    return receipt


def initialize_plan(root: Path, run_id: str) -> Path:
    validate_protocol()
    raw, _ = evidence_paths(root, run_id)
    receipt = validate_preflight(raw, run_id)
    path = raw / "capture-plan.json"
    if path.exists():
        raise EvidenceError(f"capture plan already exists: {path}")
    payload = {
        "schema_version": 1,
        "run_id": run_id,
        "commit": receipt["commit"],
        "paid_window": {
            "prepare_before_apply": True,
            "evidence_deadline_minutes": 150,
            "hard_deadline_minutes": 180,
            "destroy_starts_after_success_or_failure": True,
            "sanitize_after_destroy": True,
        },
        "failure_transition": {
            "skip_remaining_live_captures": True,
            "next_phase": "destroy_first",
            "required_followups": ["after_destroy", "after_billing_settles"],
        },
        "captures": capture_steps(),
        "screenshots": screenshot_steps(),
    }
    write_json_exclusive(path, payload, 0o600)
    return path


def parse_utc(value: str, field: str) -> datetime.datetime:
    if not isinstance(value, str) or not UTC_RE.fullmatch(value):
        raise EvidenceError(f"{field} must use UTC YYYY-MM-DDTHH:MM:SSZ")
    try:
        return datetime.datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=datetime.UTC
        )
    except ValueError as error:
        raise EvidenceError(f"{field} is not a real UTC timestamp") from error


def start_session(
    root: Path, run_id: str, started_at: str, operator: str, region: str
) -> Path:
    raw, _ = evidence_paths(root, run_id)
    receipt = validate_preflight(raw, run_id)
    if not (raw / "capture-plan.json").is_file():
        raise EvidenceError("capture-plan.json is missing; run init first")
    started = parse_utc(started_at, "BILLABLE_STARTED_AT")
    operator = operator.strip()
    if not operator or "\n" in operator:
        raise EvidenceError("M4_OPERATOR must name the executing operator")
    if region != "us-east-1":
        raise EvidenceError("M4 uses the fixed us-east-1 region")
    path = raw / "00-session.json"
    if (raw / "secret-scan.json").exists() or (raw / "secret-scan.json").is_symlink():
        raise EvidenceError("secret scan receipt already exists; run ID is spent")
    if path.exists():
        raise EvidenceError(f"session receipt already exists: {path}")
    payload = {
        "schema_version": 1,
        "run_id": run_id,
        "commit": receipt["commit"],
        "region": region,
        "operator": operator,
        "billable_started_at": started_at,
        "destroy_deadline": (started + datetime.timedelta(minutes=150))
        .isoformat()
        .replace("+00:00", "Z"),
        "hard_deadline": (started + datetime.timedelta(minutes=180))
        .isoformat()
        .replace("+00:00", "Z"),
        "limits": {"maximum_hourly_usd": 1.25, "maximum_total_usd": 5.0},
    }
    write_json_exclusive(path, payload, 0o600)
    write_json_exclusive(
        raw / "secret-scan.json",
        {
            "schema_version": 1,
            "run_id": run_id,
            "commit": receipt["commit"],
            "session_sha256": hash_file(path),
            "profile": "m4-secret-representations-v1",
            "state": "not_started",
            "entries": [],
        },
        0o600,
    )
    return path


def process_is_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def validate_live_controller(
    root: Path,
    run_id: str,
    commit: str,
    controller_pid: int,
    *,
    now: datetime.datetime | None = None,
    pid_check: Any = process_is_running,
    phase: str = "applying",
) -> dict[str, Any]:
    validate_run_id(run_id)
    if not COMMIT_RE.fullmatch(commit):
        raise EvidenceError("approved commit must be a full lowercase git SHA")
    if type(controller_pid) is not int or controller_pid <= 0:
        raise EvidenceError("hourly apply requires the live-run controller PID")
    raw, _ = evidence_paths(root, run_id)
    session = read_json_object(raw / "00-session.json", "session receipt")
    if (
        session.get("schema_version") != 1
        or session.get("run_id") != run_id
        or session.get("commit") != commit
        or session.get("region") != "us-east-1"
    ):
        raise EvidenceError("session receipt is not bound to this run and commit")
    started = parse_utc(session.get("billable_started_at"), "billable start")
    destroy = parse_utc(session.get("destroy_deadline"), "destroy deadline")
    hard = parse_utc(session.get("hard_deadline"), "hard deadline")
    if destroy - started != datetime.timedelta(minutes=150):
        raise EvidenceError("session receipt has the wrong destroy deadline")
    if hard - started != datetime.timedelta(minutes=180):
        raise EvidenceError("session receipt has the wrong hard deadline")
    state = read_json_object(raw / "controller-state.json", "controller state")
    if (
        state.get("schema_version") != 1
        or state.get("run_id") != run_id
        or state.get("commit") != commit
        or state.get("region") != "us-east-1"
        or state.get("controller_pid") != controller_pid
        or phase not in {"applying", "live"}
        or state.get("phase") != phase
        or (
            phase == "live"
            and (type(state.get("apply_exit")) is not int or state["apply_exit"] != 0)
        )
    ):
        raise EvidenceError("controller state does not authorize this apply")
    current = (now or datetime.datetime.now(datetime.UTC)).astimezone(datetime.UTC)
    if started > current + datetime.timedelta(seconds=30):
        raise EvidenceError("session start is in the future")
    if phase == "applying" and current - started > APPLY_START_MAX_AGE:
        raise EvidenceError("session is too old to start an hourly apply")
    if current >= destroy:
        raise EvidenceError("destroy deadline has already passed")
    updated = parse_utc(state.get("updated_at"), "controller heartbeat")
    if updated > current + datetime.timedelta(seconds=30):
        raise EvidenceError("controller heartbeat is in the future")
    if current - updated > datetime.timedelta(seconds=5):
        raise EvidenceError("live-run controller heartbeat is stale")
    if not pid_check(controller_pid):
        raise EvidenceError("live-run controller process is not running")
    return {
        "run_id": run_id,
        "commit": commit,
        "controller_pid": controller_pid,
        "billable_started_at": session["billable_started_at"],
        "destroy_deadline": session["destroy_deadline"],
        "hard_deadline": session["hard_deadline"],
    }


def load_redactions(path: Path) -> dict[str, str]:
    payload = read_json_object(path, "redactions file")
    if payload.get("schema_version") != 1 or not isinstance(
        payload.get("values"), dict
    ):
        raise EvidenceError(
            "redactions file needs schema_version 1 and a values object"
        )
    result: dict[str, str] = {}
    for name, value in payload["values"].items():
        if name in SECRET_NAMES:
            raise EvidenceError(
                "plaintext credential fields are forbidden; use the bootstrap secret scan receipt"
            )
        if not isinstance(name, str) or not REDACTION_NAME_RE.fullmatch(name):
            raise EvidenceError(f"invalid redaction name: {name!r}")
        if not isinstance(value, str) or len(value) < 3:
            raise EvidenceError(
                f"redaction value {name} must contain at least 3 characters"
            )
        if value in result.values():
            raise EvidenceError("redaction values must be unique")
        result[name] = value
    if not result:
        raise EvidenceError("redactions file contains no known sensitive values")
    if missing := sorted(REQUIRED_REDACTION_NAMES - set(result)):
        raise EvidenceError("redactions file is missing: " + ", ".join(missing))
    return result


def load_secret_scan(
    raw: Path, run_id: str, *, complete: bool = True
) -> dict[str, Any]:
    path = raw / "secret-scan.json"
    if (
        path.is_symlink()
        or not path.is_file()
        or path.stat().st_mode & 0o777 != 0o600
        or path.stat().st_size > 32768
    ):
        raise EvidenceError("private secret scan receipt is missing or unsafe")
    scan = read_json_object(path, "secret scan")
    preflight = validate_preflight(raw, run_id)
    if (
        set(scan)
        != {
            "schema_version",
            "run_id",
            "commit",
            "session_sha256",
            "profile",
            "state",
            "entries",
        }
        or type(scan.get("schema_version")) is not int
        or scan.get("schema_version") != 1
        or scan.get("run_id") != run_id
        or scan.get("commit") != preflight["commit"]
        or scan.get("session_sha256") != hash_file(raw / "00-session.json")
        or scan.get("profile") != "m4-secret-representations-v1"
        or not isinstance(scan.get("state"), str)
        or scan.get("state") not in {"not_started", "collecting", "complete"}
    ):
        raise EvidenceError("secret scan receipt binding or state mismatch")
    entries = scan.get("entries")
    if not isinstance(entries, list) or len(entries) > 21:
        raise EvidenceError("invalid secret scan entries")
    seen = set()
    for entry in entries:
        if not isinstance(entry, dict) or set(entry) != {
            "name",
            "representation",
            "byte_length",
            "sha256",
        }:
            raise EvidenceError("invalid secret scan entry")
        name, representation = entry["name"], entry["representation"]
        length, digest = entry["byte_length"], entry["sha256"]
        if (
            not isinstance(name, str)
            or name not in SECRET_NAMES
            or not isinstance(representation, str)
            or representation not in SECRET_REPRESENTATIONS
            or type(length) is not int
            or not 16 <= length <= 24576
            or not isinstance(digest, str)
            or not re.fullmatch(r"[0-9a-f]{64}", digest)
            or (name, representation) in seen
        ):
            raise EvidenceError("invalid or duplicate secret scan entry")
        seen.add((name, representation))
    if scan["state"] == "not_started" and entries:
        raise EvidenceError("not_started scan contains credential entries")
    if complete or scan["state"] == "complete":
        if scan["state"] != "complete" or seen != {
            (n, r) for n in SECRET_NAMES for r in SECRET_REPRESENTATIONS
        }:
            raise EvidenceError("secret scan coverage is incomplete")
    return scan


def scan_secret_bytes(content: bytes, scan: Mapping[str, Any], filename: str) -> None:
    if len(content) > 8 * 1024 * 1024:
        raise EvidenceError(f"secret scan size limit exceeded: {filename}")
    by_length: dict[int, dict[str, str]] = {}
    for entry in scan["entries"]:
        by_length.setdefault(entry["byte_length"], {})[entry["sha256"]] = entry["name"]
    pending = [(content, 0)]
    scanned = set()
    total = 0
    while pending:
        value, depth = pending.pop()
        if value in scanned:
            continue
        scanned.add(value)
        total += len(value)
        if total > 32 * 1024 * 1024:
            raise EvidenceError("decoded secret scan size limit exceeded")
        for length, digests in by_length.items():
            for start in range(len(value) - length + 1):
                digest = hashlib.sha256(value[start : start + length]).hexdigest()
                if digest in digests:
                    raise EvidenceError(
                        f"credential match {digests[digest]} in {filename}"
                    )
        # Inspect JSON string carriers as well as their serialized bytes. This
        # includes JSON log lines wrapped in a larger exported JSON artifact.
        for line in [value, *value.splitlines()]:
            line = re.sub(rb"^\[pod/[^\]\r\n]{1,512}\]\s*", b"", line)
            try:
                decoded = json.loads(line, object_pairs_hook=unique_json_object)
            except (ValueError, UnicodeDecodeError):
                continue
            if depth >= 4 and isinstance(decoded, (dict, list, str)):
                raise EvidenceError("JSON carrier exceeds secret scan depth limit")
            objects = [decoded]
            while objects:
                item = objects.pop()
                if isinstance(item, str):
                    pending.append((item.encode(), depth + 1))
                elif isinstance(item, dict):
                    # Kafka record byte fields use base64, including a JSON
                    # payload containing a credential rather than only the
                    # credential's standalone encoding. Decode known carriers.
                    if {"topic", "partition", "offset"} <= item.keys():
                        for key in ("key", "value", "raw_value"):
                            encoded = item.get(key)
                            if encoded is None:
                                continue
                            if not isinstance(encoded, str):
                                raise EvidenceError("invalid Kafka byte carrier")
                            if depth >= 4:
                                raise EvidenceError(
                                    "Kafka carrier exceeds secret scan depth limit"
                                )
                            try:
                                pending.append(
                                    (
                                        base64.b64decode(encoded, validate=True),
                                        depth + 1,
                                    )
                                )
                            except (ValueError, binascii.Error) as error:
                                raise EvidenceError(
                                    "invalid Kafka base64 carrier"
                                ) from error
                    objects.extend(item.keys())
                    objects.extend(item.values())
                elif isinstance(item, list):
                    objects.extend(item)


def scan_secret_files(
    directory: Path, names: list[str], scan: Mapping[str, Any]
) -> None:
    for name in names:
        path = directory / name
        if path.suffix == ".png":
            continue  # Pixel review is explicit and separate.
        if (
            path.is_symlink()
            or not path.is_file()
            or path.stat().st_size > 8 * 1024 * 1024
        ):
            raise EvidenceError(f"secret scan input is missing or unsafe: {name}")
        scan_secret_bytes(path.read_bytes(), scan, name)


def replace_public_ipv4(match: re.Match[str]) -> str:
    try:
        address = ipaddress.ip_address(match.group(0))
    except ValueError:
        return match.group(0)
    return "[REDACTED:PUBLIC_IP]" if address.is_global else match.group(0)


def protect_identifiers(
    text: str, redactions: Mapping[str, str]
) -> tuple[str, list[str]]:
    preserved: list[str] = []
    sensitive_spans: list[tuple[int, int]] = []
    for name, value in redactions.items():
        if name == "ACCOUNT_ID":
            continue
        start = 0
        while (index := text.find(value, start)) >= 0:
            sensitive_spans.append((index, index + len(value)))
            start = index + 1

    def protect(match: re.Match[str]) -> str:
        if any(
            match.start() < end and begin < match.end()
            for begin, end in sensitive_spans
        ):
            return match.group(0)
        preserved.append(match.group(0))
        return f"\ue000{len(preserved) - 1}\ue001"

    return PRESERVED_IDENTIFIER_RE.sub(protect, text), preserved


def restore_identifiers(text: str, preserved: list[str]) -> str:
    for index, value in enumerate(preserved):
        text = text.replace(f"\ue000{index}\ue001", value)
    return text


def sanitize_text(text: str, redactions: Mapping[str, str]) -> str:
    text, preserved = protect_identifiers(text, redactions)
    text = ECR_RE.sub("[REDACTED:ECR_REGISTRY]", text)
    text = RDS_RE.sub("[REDACTED:RDS_ENDPOINT]", text)
    text = MSK_RE.sub("[REDACTED:MSK_ENDPOINT]", text)
    text = EMAIL_RE.sub("[REDACTED:EMAIL]", text)
    text = IPV4_RE.sub(replace_public_ipv4, text)
    for name, value in sorted(
        redactions.items(), key=lambda item: len(item[1]), reverse=True
    ):
        text = text.replace(value, f"[REDACTED:{name}]")
    return restore_identifiers(text, preserved)


def sensitive_matches(text: str, redactions: Mapping[str, str]) -> list[str]:
    scan_text, _ = protect_identifiers(text, redactions)
    matches = [name for name, value in redactions.items() if value in scan_text]
    structural = {
        "ACCOUNT_ARN": ARN_ACCOUNT_RE,
        "ECR_REGISTRY": ECR_RE,
        "RDS_ENDPOINT": RDS_RE,
        "MSK_ENDPOINT": MSK_RE,
        "EMAIL": EMAIL_RE,
    }
    matches.extend(
        name for name, pattern in structural.items() if pattern.search(scan_text)
    )
    for match in IPV4_RE.finditer(scan_text):
        try:
            if ipaddress.ip_address(match.group(0)).is_global:
                matches.append("PUBLIC_IP")
        except ValueError:
            continue
    return sorted(set(matches))


def validate_png(path: Path) -> dict[str, int]:
    if path.is_symlink() or not path.is_file():
        raise EvidenceError(f"screenshot is missing or is not a regular file: {path}")
    content = path.read_bytes()
    if len(content) < 33 or content[:8] != PNG_SIGNATURE:
        raise EvidenceError(f"screenshot is not a PNG with an IHDR chunk: {path}")
    offset = len(PNG_SIGNATURE)
    width = 0
    height = 0
    chunk_index = 0
    image_data: list[bytes] = []
    saw_end = False
    while offset < len(content):
        if len(content) - offset < 12:
            raise EvidenceError(f"screenshot has a truncated PNG chunk: {path}")
        length = struct.unpack(">I", content[offset : offset + 4])[0]
        chunk_type = content[offset + 4 : offset + 8]
        data_start = offset + 8
        data_end = data_start + length
        chunk_end = data_end + 4
        if chunk_end > len(content):
            raise EvidenceError(f"screenshot has a truncated PNG chunk: {path}")
        data = content[data_start:data_end]
        expected_crc = struct.unpack(">I", content[data_end:chunk_end])[0]
        actual_crc = zlib.crc32(chunk_type + data) & 0xFFFFFFFF
        if actual_crc != expected_crc:
            raise EvidenceError(f"screenshot has a corrupt PNG chunk: {path}")
        if chunk_index == 0:
            if chunk_type != b"IHDR" or length != 13:
                raise EvidenceError(
                    f"screenshot is not a PNG with an IHDR chunk: {path}"
                )
            width, height = struct.unpack(">II", data[:8])
        elif chunk_type == b"IDAT":
            image_data.append(data)
        elif chunk_type == b"IEND":
            if length != 0 or chunk_end != len(content):
                raise EvidenceError(f"screenshot has an invalid PNG end: {path}")
            saw_end = True
            offset = chunk_end
            break
        offset = chunk_end
        chunk_index += 1
    if not image_data or not saw_end:
        raise EvidenceError(f"screenshot has no complete PNG image data: {path}")
    try:
        decoded = zlib.decompress(b"".join(image_data))
    except zlib.error as error:
        raise EvidenceError(f"screenshot has invalid PNG image data: {path}") from error
    if not decoded:
        raise EvidenceError(f"screenshot has empty PNG image data: {path}")
    if width < 640 or height < 360:
        raise EvidenceError(f"screenshot is too small ({width}x{height}): {path}")
    return {"width": width, "height": height}


def validate_visual_review(raw: Path, run_id: str) -> dict[str, Any]:
    review = read_json_object(raw / "visual-review.json", "visual review")
    reviewed = review.get("files")
    reviewer = review.get("reviewer")
    if (
        review.get("schema_version") != 1
        or review.get("run_id") != run_id
        or review.get("result") != "passed"
        or not isinstance(review.get("reviewed_at"), str)
        or not UTC_RE.fullmatch(review["reviewed_at"])
        or not isinstance(reviewer, str)
        or not reviewer.strip()
        or not isinstance(reviewed, list)
        or not all(isinstance(item, str) for item in reviewed)
        or not set(REQUIRED_SCREENSHOTS).issubset(reviewed)
    ):
        raise EvidenceError("visual review must pass for every required screenshot")
    parse_utc(review["reviewed_at"], "visual review reviewed_at")
    allowed = set(REQUIRED_SCREENSHOTS) | set(OPTIONAL_SCREENSHOTS)
    unexpected = sorted(set(reviewed) - allowed)
    if unexpected:
        raise EvidenceError(
            "visual review contains unknown screenshot(s): " + ", ".join(unexpected)
        )
    return review


def hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def required_text(phase: str) -> tuple[str, ...]:
    return FINAL_TEXT if phase == "final" else PROVISIONAL_TEXT


def sanitize_one(
    source: Path, destination: Path, redactions: Mapping[str, str]
) -> None:
    if source.is_symlink() or not source.is_file():
        raise EvidenceError(
            f"raw evidence is missing or is not a regular file: {source}"
        )
    try:
        raw_text = source.read_text(encoding="utf-8")
    except UnicodeDecodeError as error:
        raise EvidenceError(f"text evidence is not UTF-8: {source}") from error
    sanitized = sanitize_text(raw_text, redactions)
    leaks = sensitive_matches(sanitized, redactions)
    if leaks:
        raise EvidenceError(
            f"sanitized evidence still contains {', '.join(leaks)}: {source.name}"
        )
    if source.suffix == ".json":
        try:
            json.loads(sanitized)
        except json.JSONDecodeError as error:
            raise EvidenceError(
                f"sanitization broke JSON in {source.name}: {error}"
            ) from error
    destination.write_text(sanitized, encoding="utf-8")
    destination.chmod(0o644)


def validate_proof(raw: Path, run_id: str) -> dict[str, str]:
    preflight = validate_preflight(raw, run_id)
    capture = read_json_object(raw / "capture-result.json", "capture result")
    state = read_json_object(raw / "controller-state.json", "controller result")
    if (
        type(capture.get("schema_version")) is not int
        or capture["schema_version"] != 1
        or capture.get("run_id") != run_id
        or capture.get("source_commit") != preflight["commit"]
        or capture.get("environment") != "aws"
        or capture.get("worktree_clean") is not True
        or capture.get("result") != "passed"
        or not isinstance(capture.get("files"), dict)
        or set(capture["files"]) != set(CAPTURE_FILES)
    ):
        raise EvidenceError(
            "publication requires a clean passing AWS capture for this run"
        )
    for name in CAPTURE_FILES:
        path = raw / name
        if (
            path.is_symlink()
            or not path.is_file()
            or path.stat().st_size > 8 * 1024 * 1024
            or capture["files"][name] != hash_file(path)
        ):
            raise EvidenceError(f"capture export changed or is missing: {name}")
    if (
        type(state.get("schema_version")) is not int
        or state["schema_version"] != 1
        or state.get("run_id") != run_id
        or state.get("commit") != preflight["commit"]
        or state.get("phase") != "complete"
        or state.get("result") != "passed"
        or state.get("cleanup_verified") is not True
        or state.get("cleanup_overdue") is not False
        or state.get("log_cleanup_passed") is not True
        or state.get("transcript_passed") is not True
        or any(
            type(state.get(key)) is not int or state[key] != 0
            for key in (
                "apply_exit",
                "destroy_exit",
                "terraform_state_exit",
                "inventory_exit",
                "cost_exit",
            )
        )
    ):
        raise EvidenceError(
            "publication requires completed controller apply and verified cleanup"
        )
    started = parse_utc(capture.get("started_at"), "capture start")
    finished = parse_utc(capture.get("finished_at"), "capture finish")
    cleanup = parse_utc(state.get("cleanup_finished_at"), "cleanup finish")
    if (
        not started
        <= finished
        <= cleanup
        <= datetime.datetime.now(datetime.timezone.utc)
    ):
        raise EvidenceError("capture and cleanup chronology is invalid")
    return {
        name: hash_file(raw / name)
        for name in ("capture-result.json", "controller-state.json", "00-session.json")
    }


def cost_context(raw: Path, run_id: str) -> tuple[dict, datetime.datetime, dict]:
    preflight = validate_preflight(raw, run_id)
    session = read_json_object(raw / "00-session.json", "session")
    state = read_json_object(raw / "controller-state.json", "controller")
    if (
        session.get("run_id") != run_id
        or session.get("commit") != preflight["commit"]
        or state.get("run_id") != run_id
        or state.get("commit") != preflight["commit"]
        or state.get("phase") != "complete"
        or state.get("cleanup_verified") is not True
        or any(
            type(state.get(key)) is not int or state[key] != 0
            for key in ("terraform_state_exit", "inventory_exit")
        )
    ):
        raise EvidenceError(
            "final cost requires a matching session and completed cleanup"
        )
    start = parse_utc(session.get("billable_started_at"), "billable start")
    finish = parse_utc(state.get("cleanup_finished_at"), "cleanup finish")
    if finish < start or finish - start > datetime.timedelta(days=366):
        raise EvidenceError("invalid billable interval")
    account = (
        read_json_object(raw / "01-identity.txt", "identity")
        .get("aws", {})
        .get("account_id")
    )
    if not isinstance(account, str) or not re.fullmatch(r"\d{12}", account):
        raise EvidenceError("cost account binding missing")
    query = {
        "TimePeriod": {
            "Start": start.date().isoformat(),
            "End": (finish.date() + datetime.timedelta(days=1)).isoformat(),
        },
        "Granularity": "DAILY",
        "Metrics": ["UnblendedCost"],
        "GroupBy": [{"Type": "DIMENSION", "Key": "SERVICE"}],
        "Filter": {"Dimensions": {"Key": "LINKED_ACCOUNT", "Values": [account]}},
    }
    return query, finish, preflight


def validate_final_cost(raw: Path, run_id: str, value: dict | None = None) -> dict:
    value = (
        value
        if value is not None
        else read_json_object(raw / "23-cost-final.txt", "final cost JSON")
    )
    query, finish, preflight = cost_context(raw, run_id)
    if (
        not isinstance(value, dict)
        or set(value)
        != {
            "schema_version",
            "run_id",
            "source_commit",
            "session_sha256",
            "controller_sha256",
            "collected_at",
            "attribution",
            "tag_filter_used",
            "query",
            "pages",
        }
        or type(value.get("schema_version")) is not int
        or value["schema_version"] != 1
        or value.get("run_id") != run_id
        or value.get("source_commit") != preflight["commit"]
        or value.get("query") != query
        or value.get("tag_filter_used") is not False
        or value.get("attribution")
        != "account-wide daily service totals; not exact M4 attribution"
        or value.get("session_sha256") != hash_file(raw / "00-session.json")
        or value.get("controller_sha256") != hash_file(raw / "controller-state.json")
    ):
        raise EvidenceError(
            "final cost receipt does not match the session query and attribution"
        )
    collected = parse_utc(value.get("collected_at"), "cost collection")
    if (
        not finish + datetime.timedelta(hours=48)
        <= collected
        <= datetime.datetime.now(datetime.timezone.utc)
    ):
        raise EvidenceError(
            "final cost must be collected at least 48 hours after cleanup and not in the future"
        )
    pages = value.get("pages")
    if not isinstance(pages, list) or not 1 <= len(pages) <= 100:
        raise EvidenceError("final cost pages missing or excessive")
    days = {}
    tokens = set()
    for index, page in enumerate(pages):
        if (
            not isinstance(page, dict)
            or page.get("GroupDefinitions") != query["GroupBy"]
        ):
            raise EvidenceError("final cost grouping is invalid")
        token = page.get("NextPageToken")
        if index < len(pages) - 1:
            if not isinstance(token, str) or not token or token in tokens:
                raise EvidenceError("final cost pagination is incomplete or repeated")
            tokens.add(token)
        elif token:
            raise EvidenceError("final cost pagination is incomplete")
        results = page.get("ResultsByTime")
        if not isinstance(results, list) or not results:
            raise EvidenceError("final cost has no daily results")
        for result in results:
            if not isinstance(result, dict) or result.get("Estimated") is not False:
                raise EvidenceError("final cost contains estimated or invalid days")
            period = result.get("TimePeriod", {})
            try:
                day = datetime.date.fromisoformat(period["Start"])
                end = datetime.date.fromisoformat(period["End"])
            except (KeyError, TypeError, ValueError) as error:
                raise EvidenceError("invalid cost day") from error
            if end != day + datetime.timedelta(days=1):
                raise EvidenceError("cost result is not daily")
            services = days.setdefault(day.isoformat(), set())
            groups = result.get("Groups")
            if not isinstance(groups, list):
                raise EvidenceError("cost service groups missing")
            for group in groups:
                if not isinstance(group, dict) or not isinstance(
                    group.get("Metrics"), dict
                ):
                    raise EvidenceError("invalid cost service group")
                keys = group.get("Keys", [])
                metric = group.get("Metrics", {}).get("UnblendedCost", {})
                if (
                    not isinstance(keys, list)
                    or len(keys) != 1
                    or not isinstance(metric, dict)
                    or not isinstance(keys[0], str)
                    or not keys[0]
                    or keys[0] in services
                    or metric.get("Unit") != "USD"
                    or not isinstance(metric.get("Amount"), str)
                ):
                    raise EvidenceError("invalid or duplicate cost service group")
                try:
                    if not Decimal(metric["Amount"]).is_finite():
                        raise InvalidOperation
                except InvalidOperation as error:
                    raise EvidenceError("invalid cost amount") from error
                services.add(keys[0])
    first = datetime.date.fromisoformat(query["TimePeriod"]["Start"])
    last = datetime.date.fromisoformat(query["TimePeriod"]["End"])
    expected = {
        (first + datetime.timedelta(days=i)).isoformat()
        for i in range((last - first).days)
    }
    if set(days) != expected:
        raise EvidenceError("final cost days do not cover the session interval")
    return value


def private_aws_json(raw: Path, arguments: list[str]) -> dict:
    profile = read_json_object(raw / "06-go-no-go.json", "GO").get("aws_profile")
    if not isinstance(profile, str) or not profile:
        raise EvidenceError("AWS profile binding is missing")
    environment = {
        k: os.environ[k] for k in ("HOME", "PATH", "TMPDIR") if k in os.environ
    }
    environment.update(
        AWS_PROFILE=profile,
        AWS_REGION="us-east-1",
        AWS_DEFAULT_REGION="us-east-1",
        AWS_MAX_ATTEMPTS="1",
        AWS_PAGER="",
    )
    try:
        result = subprocess.run(
            [
                "aws",
                *arguments,
                "--output",
                "json",
                "--cli-connect-timeout",
                "10",
                "--cli-read-timeout",
                "30",
            ],
            env=environment,
            capture_output=True,
            timeout=45,
            check=True,
        )
        if len(result.stdout) > 8 * 1024 * 1024:
            raise EvidenceError("AWS result exceeds evidence limit")
        value = json.loads(result.stdout, object_pairs_hook=unique_json_object)
        if not isinstance(value, dict):
            raise EvidenceError("AWS result is not an object")
        return value
    except (OSError, subprocess.SubprocessError, ValueError) as error:
        raise EvidenceError(
            "read-only AWS evidence query failed; diagnostics withheld"
        ) from error


def collect_final_cost(root: Path, run_id: str) -> Path:
    raw, _ = evidence_paths(root, run_id)
    query, finish, preflight = cost_context(raw, run_id)
    now = datetime.datetime.now(datetime.timezone.utc)
    if now < finish + datetime.timedelta(hours=48):
        raise EvidenceError(
            "wait at least 48 hours after cleanup before final cost collection"
        )
    if (
        private_aws_json(raw, ["sts", "get-caller-identity"]).get("Account")
        != query["Filter"]["Dimensions"]["Values"][0]
    ):
        raise EvidenceError("cost collection identity does not match the run")
    pages, tokens = [], set()
    token = None
    for _ in range(100):
        arguments = [
            "ce",
            "get-cost-and-usage",
            "--no-paginate",
            "--cli-input-json",
            json.dumps(query),
        ]
        if token:
            arguments.extend(["--next-page-token", token])
        page = private_aws_json(raw, arguments)
        pages.append(page)
        token = page.get("NextPageToken")
        if not token:
            break
        if not isinstance(token, str) or token in tokens:
            raise EvidenceError("repeated or invalid cost pagination token")
        tokens.add(token)
    value = {
        "schema_version": 1,
        "run_id": run_id,
        "source_commit": preflight["commit"],
        "session_sha256": hash_file(raw / "00-session.json"),
        "controller_sha256": hash_file(raw / "controller-state.json"),
        "collected_at": datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        ),
        "attribution": "account-wide daily service totals; not exact M4 attribution",
        "tag_filter_used": False,
        "query": query,
        "pages": pages,
    }
    validate_final_cost(raw, run_id, value)
    path = raw / "23-cost-final.txt"
    write_json_exclusive(path, value, 0o600)
    return path


def create_publication(
    raw: Path,
    destination: Path,
    run_id: str,
    phase: str,
    redactions: Mapping[str, str],
) -> dict[str, Any]:
    review = validate_visual_review(raw, run_id)
    screenshots = list(REQUIRED_SCREENSHOTS)
    screenshots.extend(name for name in OPTIONAL_SCREENSHOTS if (raw / name).is_file())
    files: dict[str, dict[str, Any]] = {}

    for name in required_text(phase):
        target = destination / name
        sanitize_one(raw / name, target, redactions)
        files[name] = {"sha256": hash_file(target)}

    reviewed = set(review["files"])
    for name in screenshots:
        if name not in reviewed:
            raise EvidenceError(f"screenshot was not included in visual review: {name}")
        metadata = validate_png(raw / name)
        target = destination / name
        shutil.copyfile(raw / name, target)
        target.chmod(0o644)
        files[name] = {"sha256": hash_file(target), **metadata}

    return {
        "schema_version": 1,
        "run_id": run_id,
        "phase": phase,
        "result": "passed",
        "redaction_names": sorted(redactions),
        "visual_review": {
            "reviewed_at": review["reviewed_at"],
            "files": screenshots,
        },
        "files": files,
        "proof_receipts": validate_proof(raw, run_id),
        **(
            {"final_cost_sha256": hash_file(raw / "23-cost-final.txt")}
            if phase == "final"
            else {}
        ),
    }


def verify_publication(
    destination: Path,
    run_id: str,
    phase: str,
    redactions: Mapping[str, str],
    *,
    raw: Path | None = None,
) -> dict[str, Any]:
    receipt_path = destination / "publication.json"
    receipt = read_json_object(receipt_path, "publication receipt")
    files = receipt.get("files")
    visual_review = receipt.get("visual_review")
    if (
        set(receipt)
        != {
            "schema_version",
            "run_id",
            "phase",
            "result",
            "redaction_names",
            "visual_review",
            "files",
            "secret_scan_sha256",
            "proof_receipts",
        }
        | ({"final_cost_sha256"} if phase == "final" else set())
        or receipt.get("schema_version") != 1
        or receipt.get("run_id") != run_id
        or receipt.get("phase") != phase
        or receipt.get("result") != "passed"
        or receipt.get("redaction_names") != sorted(redactions)
        or not isinstance(visual_review, dict)
        or set(visual_review) != {"reviewed_at", "files"}
        or not isinstance(visual_review.get("files"), list)
        or not isinstance(files, dict)
    ):
        raise EvidenceError("publication receipt does not match this run and phase")
    raw = raw or destination.parents[3] / ".evidence" / "m4" / run_id
    if receipt["proof_receipts"] != validate_proof(raw, run_id):
        raise EvidenceError("private proof receipts changed since publication")
    if phase == "final":
        validate_final_cost(raw, run_id)
        if receipt["final_cost_sha256"] != hash_file(raw / "23-cost-final.txt"):
            raise EvidenceError("final cost changed since publication")
    receipt_text = receipt_path.read_text(encoding="utf-8")
    if leaks := sensitive_matches(receipt_text, redactions):
        raise EvidenceError(
            "publication receipt contains sensitive value(s): " + ", ".join(leaks)
        )
    expected = set(required_text(phase)) | set(REQUIRED_SCREENSHOTS)
    actual = {path.name for path in destination.iterdir()}
    allowed = expected | set(OPTIONAL_SCREENSHOTS) | {"publication.json"}
    if missing := sorted(expected - actual):
        raise EvidenceError("published evidence is missing: " + ", ".join(missing))
    if unexpected := sorted(actual - allowed):
        raise EvidenceError(
            "published evidence contains unexpected file(s): " + ", ".join(unexpected)
        )
    published_screenshots = [
        name for name in REQUIRED_SCREENSHOTS + OPTIONAL_SCREENSHOTS if name in actual
    ]
    if visual_review["files"] != published_screenshots:
        raise EvidenceError(
            "publication receipt screenshot list does not match the directory"
        )
    parse_utc(visual_review["reviewed_at"], "publication visual review reviewed_at")
    if set(files) != actual - {"publication.json"}:
        raise EvidenceError(
            "publication receipt file list does not match the directory"
        )

    for name, metadata in files.items():
        path = destination / name
        if path.is_symlink() or not path.is_file():
            raise EvidenceError(f"published evidence is not a regular file: {name}")
        if not isinstance(metadata, dict) or metadata.get("sha256") != hash_file(path):
            raise EvidenceError(f"published evidence hash changed: {name}")
        if path.suffix == ".png":
            validate_png(path)
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError as error:
            raise EvidenceError(f"published evidence is not UTF-8: {name}") from error
        if leaks := sensitive_matches(text, redactions):
            raise EvidenceError(
                f"published evidence contains {', '.join(leaks)}: {name}"
            )
        if path.suffix == ".json":
            try:
                json.loads(text)
            except json.JSONDecodeError as error:
                raise EvidenceError(
                    f"published JSON is invalid in {name}: {error}"
                ) from error
    return receipt


def publish(root: Path, run_id: str, phase: str, redactions_path: Path) -> Path:
    if phase == "failed":
        return publish_failed(root, run_id, redactions_path)
    raw, destination = evidence_paths(root, run_id)
    validate_preflight(raw, run_id)
    if not (raw / "capture-plan.json").is_file():
        raise EvidenceError("capture-plan.json is missing; run init first")
    redactions = load_redactions(redactions_path)
    scan = load_secret_scan(raw, run_id)
    scan_names = list(required_text(phase))
    if (raw / "application-logs.txt").exists():
        scan_names.append("application-logs.txt")
    scan_secret_files(raw, scan_names, scan)
    validate_proof(raw, run_id)
    if phase == "final":
        validate_final_cost(raw, run_id)

    if destination.exists():
        if phase != "final":
            raise EvidenceError(
                f"published evidence destination already exists: {destination}"
            )
        verify_publication(destination, run_id, "provisional", redactions)
        verify_secret_publication(raw, destination, run_id)
        target = destination / "23-cost-final.txt"
        if target.exists():
            raise EvidenceError(f"final cost evidence already exists: {target}")
        sanitize_one(raw / target.name, target, redactions)
        old = read_json_object(destination / "publication.json", "publication receipt")
        old["phase"] = "final"
        old["final_cost_sha256"] = hash_file(raw / "23-cost-final.txt")
        old["files"][target.name] = {"sha256": hash_file(target)}
        receipt_path = destination / "publication.json"
        write_json_atomic(receipt_path, old, 0o644)
        verify_publication(destination, run_id, "final", redactions)
        verify_secret_publication(raw, destination, run_id)
        return destination

    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{run_id}.", dir=destination.parent))
    try:
        receipt = create_publication(raw, temporary, run_id, phase, redactions)
        receipt["secret_scan_sha256"] = hash_file(raw / "secret-scan.json")
        write_json_exclusive(temporary / "publication.json", receipt, 0o644)
        verify_publication(temporary, run_id, phase, redactions)
        verify_secret_publication(raw, temporary, run_id)
        os.replace(temporary, destination)
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return destination


def verify_secret_publication(raw: Path, destination: Path, run_id: str) -> None:
    scan = load_secret_scan(raw, run_id)
    publication = read_json_object(destination / "publication.json", "publication")
    if publication.get("secret_scan_sha256") != hash_file(raw / "secret-scan.json"):
        raise EvidenceError("private secret scan changed since publication")
    scan_secret_files(destination, [p.name for p in destination.iterdir()], scan)


def failed_summary(raw: Path, run_id: str) -> dict[str, Any]:
    scan = load_secret_scan(raw, run_id, complete=False)
    preflight = validate_preflight(raw, run_id)
    state = read_json_object(raw / "controller-state.json", "controller result")
    if (
        state.get("schema_version") != 1
        or state.get("run_id") != run_id
        or state.get("commit") != preflight["commit"]
        or state.get("phase")
        not in {
            "applying",
            "live",
            "destroying",
            "verifying",
            "complete",
            "cleanup_failed",
            "verifying_identity",
        }
    ):
        raise EvidenceError(
            "failed publication requires a controller observation for this run"
        )
    # Never copy free-text diagnostics when bootstrap coverage is incomplete.
    # This allowlist also keeps operator names, raw logs and endpoints private.
    checks = {}
    for key in (
        "apply_exit",
        "destroy_exit",
        "terraform_state_exit",
        "inventory_exit",
        "cost_exit",
    ):
        value = state.get(key)
        if value is not None and (type(value) is not int or not -1 <= value <= 255):
            raise EvidenceError("terminal controller has an invalid exit status")
        checks[key] = {
            "reported_exit": value,
            "status": ("passed" if value == 0 else "failed")
            if value is not None
            else (
                "not_run"
                if state.get("cleanup_blocked_reason") == "identity_unverified"
                and key != "apply_exit"
                else "unknown"
            ),
        }
    for key in (
        "cleanup_verified",
        "cleanup_overdue",
        "log_cleanup_passed",
        "transcript_passed",
    ):
        value = state.get(key)
        if value is not None and type(value) is not bool:
            raise EvidenceError("terminal controller has incomplete cleanup status")
        checks[key] = value
    finished = state.get("cleanup_finished_at")
    if finished:
        parse_utc(finished, "cleanup completion")
    checks["cleanup_verified"] = bool(
        state.get("phase") == "complete"
        and finished
        and checks["cleanup_verified"] is True
        and state.get("terraform_state_exit") == 0
        and state.get("inventory_exit") == 0
    )
    if not finished:
        checks["cleanup_overdue"] = None
    return {
        "schema_version": 1,
        "run_id": run_id,
        "source_commit": preflight["commit"],
        "result": "failed",
        "demonstration_passed": False,
        "controller_phase": state["phase"],
        "cleanup_finished_at": finished,
        "checks": checks,
        "secret_scan_state": scan["state"],
        "publication_scope": "last controller observation; unknown outcomes are not teardown proof; raw diagnostics and screenshots withheld",
    }


def snapshot_failed_attempt(raw: Path, run_id: str) -> Path:
    destination = raw / "failed-publication"
    ensure_beneath(destination, raw)
    if destination.exists():
        failed_summary(destination, run_id)
        return destination
    temporary = Path(tempfile.mkdtemp(prefix=".failed-publication-", dir=raw))
    try:
        for name in (
            "00-preflight.json",
            "00-session.json",
            "01-identity.txt",
            "06-go-no-go.json",
            "secret-scan.json",
            "controller-state.json",
        ):
            source = raw / name
            if (
                source.is_symlink()
                or not source.is_file()
                or source.stat().st_size > 1024 * 1024
            ):
                raise EvidenceError("failed snapshot source missing or unsafe")
            shutil.copyfile(source, temporary / name)
            (temporary / name).chmod(0o600)
        failed_summary(temporary, run_id)
        write_json_exclusive(
            temporary / "observation.json",
            {
                "observed_at": datetime.datetime.now(datetime.timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                )
            },
            0o600,
        )
        os.rename(temporary, destination)
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return destination


def publish_failed(root: Path, run_id: str, redactions_path: Path) -> Path:
    raw, destination = evidence_paths(root, run_id)
    redactions = load_redactions(redactions_path)
    raw = snapshot_failed_attempt(raw, run_id)
    summary = failed_summary(raw, run_id)
    scan = load_secret_scan(raw, run_id, complete=False)
    if destination.exists():
        raise EvidenceError("failed evidence destination already exists")
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{run_id}.", dir=destination.parent))
    try:
        write_json_exclusive(temporary / "failed-attempt.json", summary, 0o644)
        receipt = {
            "schema_version": 1,
            "run_id": run_id,
            "phase": "failed",
            "result": "failed",
            "secret_scan_sha256": hash_file(raw / "secret-scan.json"),
            "controller_snapshot_sha256": hash_file(raw / "controller-state.json"),
            "authority_sha256": {
                name: hash_file(raw / name)
                for name in ("01-identity.txt", "06-go-no-go.json")
            },
            "observed_at": read_json_object(raw / "observation.json", "observation")[
                "observed_at"
            ],
            "files": {
                "failed-attempt.json": {
                    "sha256": hash_file(temporary / "failed-attempt.json")
                }
            },
        }
        write_json_exclusive(temporary / "publication.json", receipt, 0o644)
        scan_secret_files(temporary, ["failed-attempt.json", "publication.json"], scan)
        for path in temporary.iterdir():
            if sensitive_matches(path.read_text(), redactions):
                raise EvidenceError("failed summary contains a known sensitive value")
        os.replace(temporary, destination)
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    verify_failed(root, run_id, redactions)
    return destination


def verify_failed(root: Path, run_id: str, redactions: Mapping[str, str]) -> None:
    raw, destination = evidence_paths(root, run_id)
    raw = raw / "failed-publication"
    ensure_beneath(raw, root)
    if {p.name for p in destination.iterdir()} != {
        "failed-attempt.json",
        "publication.json",
    }:
        raise EvidenceError("unexpected failed publication contents")
    scan = load_secret_scan(raw, run_id, complete=False)
    scan_secret_files(destination, ["failed-attempt.json", "publication.json"], scan)
    expected = {
        "schema_version": 1,
        "run_id": run_id,
        "phase": "failed",
        "result": "failed",
        "secret_scan_sha256": hash_file(raw / "secret-scan.json"),
        "controller_snapshot_sha256": hash_file(raw / "controller-state.json"),
        "authority_sha256": {
            name: hash_file(raw / name)
            for name in ("01-identity.txt", "06-go-no-go.json")
        },
        "observed_at": read_json_object(raw / "observation.json", "observation")[
            "observed_at"
        ],
        "files": {
            "failed-attempt.json": {
                "sha256": hash_file(destination / "failed-attempt.json")
            }
        },
    }
    if read_json_object(
        destination / "publication.json", "publication"
    ) != expected or read_json_object(
        destination / "failed-attempt.json", "failed attempt"
    ) != failed_summary(raw, run_id):
        raise EvidenceError(
            "failed publication does not match the private controller evidence"
        )
    for path in destination.iterdir():
        if sensitive_matches(path.read_text(), redactions):
            raise EvidenceError("failed summary contains a known sensitive value")


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    commands = result.add_subparsers(dest="command", required=True)
    commands.add_parser(
        "check-protocol", help="validate the account-independent capture contract"
    )
    cost = commands.add_parser(
        "collect-final-cost", help="collect settled daily account costs (read-only AWS)"
    )
    cost.add_argument("--run-id", required=True)
    init = commands.add_parser(
        "init", help="write the fixed capture and screenshot plan"
    )
    init.add_argument("--run-id", default=os.environ.get("AWS_RUN_ID", ""))

    session = commands.add_parser(
        "start-session", help="write the paid-window deadlines"
    )
    session.add_argument("--run-id", default=os.environ.get("AWS_RUN_ID", ""))
    session.add_argument("--started-at", required=True)
    session.add_argument("--operator", required=True)
    session.add_argument("--region", default="us-east-1")

    controller = commands.add_parser(
        "check-controller", help="verify the active controller for an hourly apply"
    )
    controller.add_argument("--run-id", required=True)
    controller.add_argument("--commit", required=True)
    controller.add_argument("--controller-pid", type=int, required=True)

    publish_parser = commands.add_parser("publish", help="sanitize and verify evidence")
    publish_parser.add_argument("--run-id", default=os.environ.get("AWS_RUN_ID", ""))
    publish_parser.add_argument(
        "--phase", choices=("provisional", "final", "failed"), required=True
    )
    publish_parser.add_argument("--redactions-file", type=Path, required=True)

    verify = commands.add_parser("verify", help="recheck a published evidence packet")
    verify.add_argument("--run-id", default=os.environ.get("AWS_RUN_ID", ""))
    verify.add_argument(
        "--phase", choices=("provisional", "final", "failed"), required=True
    )
    verify.add_argument("--redactions-file", type=Path, required=True)
    return result


def main() -> int:
    args = parser().parse_args()
    try:
        if args.command == "check-protocol":
            print(json.dumps(validate_protocol(), indent=2, sort_keys=True))
        elif args.command == "collect-final-cost":
            path = collect_final_cost(ROOT, args.run_id)
            print(f"settled shared-account cost receipt: {path.relative_to(ROOT)}")
        elif args.command == "init":
            path = initialize_plan(ROOT, args.run_id)
            print(f"capture plan: {path.relative_to(ROOT)}")
        elif args.command == "start-session":
            path = start_session(
                ROOT, args.run_id, args.started_at, args.operator, args.region
            )
            print(f"session receipt: {path.relative_to(ROOT)}")
        elif args.command == "check-controller":
            validate_live_controller(
                ROOT, args.run_id, args.commit, args.controller_pid
            )
            print("live-run controller permit passed")
        elif args.command == "publish":
            path = publish(ROOT, args.run_id, args.phase, args.redactions_file)
            print(
                f"{args.phase} evidence published and verified: {path.relative_to(ROOT)}"
            )
        else:
            raw, destination = evidence_paths(ROOT, args.run_id)
            redactions = load_redactions(args.redactions_file)
            if args.phase == "failed":
                verify_failed(ROOT, args.run_id, redactions)
                print("failed attempt publication verified; demonstration did not pass")
                return 0
            verify_publication(destination, args.run_id, args.phase, redactions)
            verify_secret_publication(raw, destination, args.run_id)
            print(f"{args.phase} evidence passed: {destination.relative_to(ROOT)}")
    except EvidenceError as error:
        print(f"m4 evidence: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
