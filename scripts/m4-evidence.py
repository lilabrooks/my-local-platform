#!/usr/bin/env python3
"""Plan, sanitize, and verify the M4 evidence packet."""

from __future__ import annotations

import argparse
import datetime
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import shutil
import struct
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
    "DATABASE_PASSWORD",
    "OPERATOR",
    "SIGNING_SECRET",
}
UTC_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"

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


def read_json_object(path: Path, description: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
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
        os.replace(temporary, path)
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
    return [
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
            "command": "make aws-plan AWS_RUN_ID=$AWS_RUN_ID AWS_APPROVED_COMMIT=$AWS_APPROVED_COMMIT AWS_PLAN_SUMMARY=$RAW_EVIDENCE/03-plan-summary.json",
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
            "command": "python3 scripts/m4-evidence.py start-session --run-id $AWS_RUN_ID --started-at $BILLABLE_STARTED_AT --operator $M4_OPERATOR",
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
            "source": "Prometheus instant and range queries",
            "command": "python3 scripts/m4-live-capture.py metrics --prometheus-url $PROMETHEUS_URL --output $RAW_EVIDENCE/12-metrics.txt",
            "implemented_by": "#97",
            "queries": [
                'max(relay_consumer_group_lag_total{group="relay-deliver"})',
                'max(relay_consumer_group_members{group="relay-deliver"})',
                'sum(relay_consumer_assigned_partitions{group="relay-deliver"})',
                'sum(relay_consumer_idle_members{group="relay-deliver"})',
                'count(relay_build_info{role="deliver"})',
                "sum(relay_dead_letters_total) or vector(0)",
                'sum(relay_deliveries_total{outcome="delivered"}) or vector(0)',
            ],
        },
        {
            "order": 13,
            "phase": "live_window",
            "output": "13-trace.json",
            "source": "Tempo trace API using the trace id printed by smoke",
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
            "source": "state-backed Terraform destroy transcript",
            "command": "make aws-down >$RAW_EVIDENCE/20-destroy.txt 2>&1",
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
            "source": "final attributed cost, at least 48 hours after destroy",
            "command": "make aws-cost >$RAW_EVIDENCE/23-cost-final.txt",
        },
    ]


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
    if not UTC_RE.fullmatch(value):
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
    return path


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
    }


def verify_publication(
    destination: Path,
    run_id: str,
    phase: str,
    redactions: Mapping[str, str],
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
        }
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
    raw, destination = evidence_paths(root, run_id)
    validate_preflight(raw, run_id)
    if not (raw / "capture-plan.json").is_file():
        raise EvidenceError("capture-plan.json is missing; run init first")
    redactions = load_redactions(redactions_path)

    if destination.exists():
        if phase != "final":
            raise EvidenceError(
                f"published evidence destination already exists: {destination}"
            )
        verify_publication(destination, run_id, "provisional", redactions)
        target = destination / "23-cost-final.txt"
        if target.exists():
            raise EvidenceError(f"final cost evidence already exists: {target}")
        sanitize_one(raw / target.name, target, redactions)
        old = read_json_object(destination / "publication.json", "publication receipt")
        old["phase"] = "final"
        old["files"][target.name] = {"sha256": hash_file(target)}
        receipt_path = destination / "publication.json"
        write_json_atomic(receipt_path, old, 0o644)
        verify_publication(destination, run_id, "final", redactions)
        return destination

    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{run_id}.", dir=destination.parent))
    try:
        receipt = create_publication(raw, temporary, run_id, phase, redactions)
        write_json_exclusive(temporary / "publication.json", receipt, 0o644)
        verify_publication(temporary, run_id, phase, redactions)
        os.replace(temporary, destination)
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return destination


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    commands = result.add_subparsers(dest="command", required=True)
    commands.add_parser(
        "check-protocol", help="validate the account-independent capture contract"
    )
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

    publish_parser = commands.add_parser("publish", help="sanitize and verify evidence")
    publish_parser.add_argument("--run-id", default=os.environ.get("AWS_RUN_ID", ""))
    publish_parser.add_argument(
        "--phase", choices=("provisional", "final"), required=True
    )
    publish_parser.add_argument("--redactions-file", type=Path, required=True)

    verify = commands.add_parser("verify", help="recheck a published evidence packet")
    verify.add_argument("--run-id", default=os.environ.get("AWS_RUN_ID", ""))
    verify.add_argument("--phase", choices=("provisional", "final"), required=True)
    verify.add_argument("--redactions-file", type=Path, required=True)
    return result


def main() -> int:
    args = parser().parse_args()
    try:
        if args.command == "check-protocol":
            print(json.dumps(validate_protocol(), indent=2, sort_keys=True))
        elif args.command == "init":
            path = initialize_plan(ROOT, args.run_id)
            print(f"capture plan: {path.relative_to(ROOT)}")
        elif args.command == "start-session":
            path = start_session(
                ROOT, args.run_id, args.started_at, args.operator, args.region
            )
            print(f"session receipt: {path.relative_to(ROOT)}")
        elif args.command == "publish":
            path = publish(ROOT, args.run_id, args.phase, args.redactions_file)
            print(f"{args.phase} evidence passed: {path.relative_to(ROOT)}")
        else:
            _, destination = evidence_paths(ROOT, args.run_id)
            redactions = load_redactions(args.redactions_file)
            verify_publication(destination, args.run_id, args.phase, redactions)
            print(f"{args.phase} evidence passed: {destination.relative_to(ROOT)}")
    except EvidenceError as error:
        print(f"m4 evidence: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
