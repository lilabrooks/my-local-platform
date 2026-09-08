#!/usr/bin/env python3
"""Write a redaction-safe summary of the reviewed M4 Terraform plan."""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
from typing import Any, Iterable


HOURLY_RESOURCE_TYPES = {
    "aws_db_instance",
    "aws_eks_cluster",
    "aws_eks_node_group",
    "aws_msk_serverless_cluster",
    "aws_nat_gateway",
}

REVIEWED_RESOURCE_TYPES = HOURLY_RESOURCE_TYPES | {
    "aws_ecr_repository",
    "aws_kms_alias",
    "aws_kms_key",
}
RUN_ID_RE = re.compile(r"^[0-9]{8}T[0-9]{6}Z$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
ROOT = Path(__file__).resolve().parent.parent
SUMMARY_NAME = "03-plan-summary.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("plan", type=Path)
    parser.add_argument("summary", type=Path)
    parser.add_argument("--terraform-directory", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--terraform-input-sha256", required=True)
    parser.add_argument("--verification-output", action="store_true")
    return parser.parse_args()


def validate_summary_destination(
    run_id: str, path: Path, verification_output: bool
) -> Path:
    expected_parent = ROOT / ".evidence" / "m4" / run_id
    current = expected_parent
    while current != ROOT:
        if current.is_symlink():
            raise SystemExit(f"evidence path contains a symlink: {current}")
        if ROOT not in current.parents:
            raise SystemExit("evidence path escapes the repository")
        current = current.parent
    destination = path.absolute()
    valid_name = destination.name == SUMMARY_NAME
    if verification_output:
        valid_name = destination.name.startswith(f"{SUMMARY_NAME}.verify.")
    if destination.parent != expected_parent.absolute() or not valid_name:
        raise SystemExit(
            f"summary must be {SUMMARY_NAME} directly under .evidence/m4/{run_id}"
        )
    if destination.is_symlink():
        raise SystemExit(f"summary must not be a symlink: {destination}")
    return destination


def terraform_plan_json(terraform_directory: Path, plan: Path) -> dict[str, Any]:
    result = subprocess.run(
        ["terraform", f"-chdir={terraform_directory}", "show", "-json", str(plan)],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if result.returncode != 0:
        sys.stderr.buffer.write(result.stderr)
        raise SystemExit("terraform show failed")
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise SystemExit(f"terraform show returned invalid JSON: {error}") from error


def modules(root: dict[str, Any]) -> Iterable[dict[str, Any]]:
    yield root
    for child in root.get("child_modules", []):
        yield from modules(child)


def planned_resource_type_counts(plan: dict[str, Any]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    root = plan.get("planned_values", {}).get("root_module", {})
    for module in modules(root):
        for resource in module.get("resources", []):
            counts[resource["type"]] += 1
    return dict(sorted(counts.items()))


def create_resource_type_counts(plan: dict[str, Any]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for change in plan.get("resource_changes", []):
        actions = change.get("change", {}).get("actions", [])
        if "create" in actions:
            counts[change["type"]] += 1
    return dict(sorted(counts.items()))


def hourly_counts(type_counts: dict[str, int]) -> dict[str, int]:
    return {
        resource_type: type_counts.get(resource_type, 0)
        for resource_type in HOURLY_RESOURCE_TYPES
    }


def selected_changes(
    plan: dict[str, Any], resource_types: set[str]
) -> list[dict[str, Any]]:
    changes = []
    for change in plan.get("resource_changes", []):
        if change.get("type") not in resource_types:
            continue
        actions = change.get("change", {}).get("actions", [])
        if actions == ["no-op"]:
            continue
        changes.append(
            {
                "address": change.get("address"),
                "type": change.get("type"),
                "actions": actions,
            }
        )
    return sorted(changes, key=lambda item: item["address"])


def hourly_changes(plan: dict[str, Any]) -> list[dict[str, Any]]:
    return selected_changes(plan, HOURLY_RESOURCE_TYPES)


def required_output(plan: dict[str, Any], name: str) -> Any:
    output = plan.get("planned_values", {}).get("outputs", {}).get(name)
    if not output or "value" not in output:
        raise SystemExit(f"plan is missing required output {name!r}")
    return output["value"]


def gate_failures(
    shape: dict[str, Any],
    planned: dict[str, int],
    creates: dict[str, int],
    changes: list[dict[str, Any]],
) -> list[str]:
    failures = []
    enabled = {
        "aws_db_instance": bool(shape["enable_rds"]),
        "aws_eks_cluster": bool(shape["enable_eks"]),
        "aws_eks_node_group": bool(shape["enable_eks"]),
        "aws_msk_serverless_cluster": bool(shape["enable_msk"]),
        "aws_nat_gateway": bool(shape["enable_eks"]),
    }

    for resource_type in HOURLY_RESOURCE_TYPES:
        if enabled[resource_type] and planned[resource_type] != 1:
            failures.append(
                f"{resource_type} planned count {planned[resource_type]} must be 1 when enabled"
            )
        if not enabled[resource_type] and planned[resource_type] != 0:
            failures.append(f"{resource_type} is planned while its flag is disabled")
        if not enabled[resource_type] and creates[resource_type] != 0:
            failures.append(f"{resource_type} is created while its flag is disabled")

    for change in changes:
        if enabled[change["type"]] and "delete" in change["actions"]:
            failures.append(
                f"{change['address']} would delete an enabled hourly resource"
            )

    if shape["expected_hourly_usd"] > shape["maximum_hourly_usd"]:
        failures.append(
            f"expected hourly cost {shape['expected_hourly_usd']:.4f} exceeds "
            f"{shape['maximum_hourly_usd']:.2f}"
        )

    kafka = shape["kafka"]
    if kafka["total_partitions"] > 13:
        failures.append(f"Kafka partition count {kafka['total_partitions']} exceeds 13")

    eks = shape["eks"]
    if shape["enable_eks"] and eks["node_capacity_type"] != "SPOT":
        failures.append("EKS node capacity type is not SPOT")
    if shape["enable_eks"] and eks["node_maximum"] > 3:
        failures.append(f"EKS node maximum {eks['node_maximum']} exceeds 3")
    if shape["enable_eks"] and eks["node_desired"] > eks["node_maximum"]:
        failures.append("EKS desired node count exceeds its maximum")

    return failures


def write_summary(path: Path, summary: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.parent.chmod(0o700)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", dir=path.parent
    )
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as temporary:
            json.dump(summary, temporary, indent=2, sort_keys=True)
            temporary.write("\n")
        os.replace(temporary_name, path)
    except BaseException:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise


def main() -> int:
    args = parse_args()
    if not RUN_ID_RE.fullmatch(args.run_id):
        raise SystemExit("run id must use UTC YYYYMMDDTHHMMSSZ")
    if not COMMIT_RE.fullmatch(args.source_commit):
        raise SystemExit("source commit must be a full lowercase git SHA")
    if not re.fullmatch(r"[0-9a-f]{64}", args.terraform_input_sha256):
        raise SystemExit("Terraform input digest must be a lowercase SHA-256")
    summary_path = validate_summary_destination(
        args.run_id, args.summary, args.verification_output
    )
    plan_path = args.plan.resolve()
    plan = terraform_plan_json(args.terraform_directory.resolve(), plan_path)
    if plan.get("complete") is not True:
        raise SystemExit("plan is incomplete; targeted and destroy plans are rejected")
    shape = required_output(plan, "runtime_shape")
    budget_name = required_output(plan, "runtime_budget_name")
    all_planned = planned_resource_type_counts(plan)
    all_creates = create_resource_type_counts(plan)
    planned = hourly_counts(all_planned)
    creates = hourly_counts(all_creates)
    changed_hourly_resources = hourly_changes(plan)
    failures = gate_failures(shape, planned, creates, changed_hourly_resources)

    summary = {
        "schema_version": 1,
        "run_id": args.run_id,
        "source_commit": args.source_commit,
        "terraform_input_sha256": args.terraform_input_sha256,
        "captured_at": datetime.fromtimestamp(
            plan_path.stat().st_mtime, tz=timezone.utc
        ).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "plan_sha256": hashlib.sha256(plan_path.read_bytes()).hexdigest(),
        "budget_name": budget_name,
        "shape": shape,
        "planned_resource_count": sum(all_planned.values()),
        "created_resource_count": sum(all_creates.values()),
        "planned_resource_type_counts": all_planned,
        "created_resource_type_counts": all_creates,
        "planned_hourly_resource_counts": planned,
        "created_hourly_resource_counts": creates,
        "hourly_resource_changes": changed_hourly_resources,
        "reviewed_resource_changes": selected_changes(plan, REVIEWED_RESOURCE_TYPES),
        "gate": {"passed": not failures, "failures": failures},
    }
    write_summary(summary_path, summary)

    if failures:
        for failure in failures:
            print(f"plan gate: {failure}", file=sys.stderr)
        return 1
    print(f"plan gate passed; safe summary: {args.summary}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
