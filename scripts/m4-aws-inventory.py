#!/usr/bin/env python3
"""Capture the M4 AWS inventory without relying on tags alone."""

from __future__ import annotations

import argparse
from collections.abc import Callable
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
from typing import Any


PROJECT = "my-local-platform"
NAME_PREFIX = "mlp-"
RUNTIME_SERVICES = (
    "eks",
    "msk",
    "rds",
    "ec2",
    "ebs",
    "eip",
    "elb",
    "nat",
    "logs",
)
ROOT = Path(__file__).resolve().parent.parent
RUN_ID_RE = re.compile(r"^[0-9]{8}T[0-9]{6}Z$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
OUTPUT_NAMES = {"04-inventory-before.json", "21-inventory-after.json"}


class InventoryError(RuntimeError):
    """Raised when AWS does not return a complete inventory."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument(
        "--region",
        default=os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "")),
    )
    parser.add_argument(
        "--require-no-runtime",
        action="store_true",
        help="return non-zero when an M4 runtime resource remains",
    )
    return parser.parse_args()


def validate_destination(run_id: str, output: Path) -> Path:
    if not RUN_ID_RE.fullmatch(run_id):
        raise InventoryError("run id must use UTC YYYYMMDDTHHMMSSZ")
    try:
        parsed = datetime.strptime(run_id, "%Y%m%dT%H%M%SZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError as error:
        raise InventoryError("run id is not a real UTC date and time") from error
    if parsed.strftime("%Y%m%dT%H%M%SZ") != run_id:
        raise InventoryError("run id is not a canonical UTC timestamp")

    expected_parent = ROOT / ".evidence" / "m4" / run_id
    current = expected_parent
    while current != ROOT:
        if current.is_symlink():
            raise InventoryError(f"evidence path contains a symlink: {current}")
        if ROOT not in current.parents:
            raise InventoryError("evidence path escapes the repository")
        current = current.parent
    destination = output.absolute()
    if (
        destination.parent != expected_parent.absolute()
        or destination.name not in OUTPUT_NAMES
    ):
        raise InventoryError(
            "output must be 04-inventory-before.json or 21-inventory-after.json "
            f"directly under .evidence/m4/{run_id}"
        )
    if destination.is_symlink():
        raise InventoryError(f"evidence file must not be a symlink: {destination}")
    return destination


def require_preflight(run_id: str, commit: str) -> None:
    if not COMMIT_RE.fullmatch(commit):
        raise InventoryError("approved commit must be a full lowercase git SHA")
    path = ROOT / ".evidence" / "m4" / run_id / "00-preflight.json"
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise InventoryError(f"preflight receipt is missing: {path}") from error
    except json.JSONDecodeError as error:
        raise InventoryError(f"preflight receipt is invalid JSON: {path}") from error
    if (
        not isinstance(value, dict)
        or value.get("schema_version") != 1
        or value.get("run_id") != run_id
        or value.get("result") != "passed"
        or value.get("commit") != commit
    ):
        raise InventoryError("preflight receipt is not a pass for the approved commit")


def aws_json(arguments: list[str]) -> dict[str, Any]:
    environment = os.environ.copy()
    environment["AWS_PAGER"] = ""
    result = subprocess.run(
        ["aws", *arguments, "--output", "json"],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=environment,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or "no error detail"
        raise InventoryError(f"aws {' '.join(arguments[:2])} failed: {detail}")
    try:
        value = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise InventoryError(
            f"aws {' '.join(arguments[:2])} returned invalid JSON"
        ) from error
    if not isinstance(value, dict):
        raise InventoryError(f"aws {' '.join(arguments[:2])} returned a non-object")
    return value


def project_tagged(tags: list[dict[str, Any]]) -> bool:
    for tag in tags:
        key = tag.get("Key")
        value = tag.get("Value")
        if key == "Project" and value == PROJECT:
            return True
        if isinstance(key, str) and key.startswith("kubernetes.io/cluster/mlp-"):
            return True
        if key == "Name" and project_name(value):
            return True
    return False


def project_name(value: Any) -> bool:
    return isinstance(value, str) and NAME_PREFIX in value


def instance_tags(instance: dict[str, Any]) -> list[dict[str, Any]]:
    tags = instance.get("Tags", [])
    return tags if isinstance(tags, list) else []


def collect(
    region: str,
    runner: Callable[[list[str]], dict[str, Any]] = aws_json,
) -> dict[str, Any]:
    if not region:
        raise InventoryError("AWS_REGION or --region is required")

    common = ["--region", region]
    tagged = runner(
        [
            "resourcegroupstaggingapi",
            "get-resources",
            "--tag-filters",
            f"Key=Project,Values={PROJECT}",
            *common,
        ]
    )
    eks = runner(["eks", "list-clusters", *common])
    msk = runner(["kafka", "list-clusters-v2", *common])
    rds = runner(["rds", "describe-db-instances", *common])
    ec2 = runner(
        [
            "ec2",
            "describe-instances",
            "--filters",
            "Name=instance-state-name,Values=pending,running,shutting-down,stopping,stopped",
            *common,
        ]
    )
    ebs = runner(["ec2", "describe-volumes", *common])
    eip = runner(["ec2", "describe-addresses", *common])
    elb = runner(["elbv2", "describe-load-balancers", *common])
    ecr = runner(["ecr", "describe-repositories", *common])
    nat = runner(["ec2", "describe-nat-gateways", *common])
    logs = runner(["logs", "describe-log-groups", *common])

    tagged_resources = tagged.get("ResourceTagMappingList", [])
    tagged_arns = {
        resource.get("ResourceARN")
        for resource in tagged_resources
        if isinstance(resource.get("ResourceARN"), str)
    }
    instances = [
        instance
        for reservation in ec2.get("Reservations", [])
        for instance in reservation.get("Instances", [])
        if project_tagged(instance_tags(instance))
    ]
    resources: dict[str, list[Any]] = {
        "tagged": tagged_resources,
        "eks": [name for name in eks.get("clusters", []) if project_name(name)],
        "msk": [
            cluster
            for cluster in msk.get("ClusterInfoList", [])
            if project_name(cluster.get("ClusterName"))
            or cluster.get("ClusterArn") in tagged_arns
        ],
        "rds": [
            database
            for database in rds.get("DBInstances", [])
            if project_name(database.get("DBInstanceIdentifier"))
            or database.get("DBInstanceArn") in tagged_arns
        ],
        "ec2": instances,
        "ebs": [
            volume
            for volume in ebs.get("Volumes", [])
            if project_tagged(volume.get("Tags", []))
        ],
        "eip": [
            address
            for address in eip.get("Addresses", [])
            if project_tagged(address.get("Tags", []))
        ],
        "elb": [
            load_balancer
            for load_balancer in elb.get("LoadBalancers", [])
            if project_name(load_balancer.get("LoadBalancerName"))
            or load_balancer.get("LoadBalancerArn") in tagged_arns
        ],
        "ecr": [
            repository
            for repository in ecr.get("repositories", [])
            if project_name(repository.get("repositoryName"))
            or repository.get("repositoryArn") in tagged_arns
        ],
        "nat": [
            gateway
            for gateway in nat.get("NatGateways", [])
            if project_tagged(gateway.get("Tags", []))
            and gateway.get("State") != "deleted"
        ],
        "logs": [
            group
            for group in logs.get("logGroups", [])
            if project_name(group.get("logGroupName"))
            or group.get("arn") in tagged_arns
        ],
    }
    counts = {name: len(values) for name, values in resources.items()}
    present = {name: counts[name] for name in RUNTIME_SERVICES if counts[name]}
    return {
        "schema_version": 1,
        "captured_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "region": region,
        "project": PROJECT,
        "counts": counts,
        "runtime_resources_present": present,
        "runtime_empty": not present,
        "resources": resources,
    }


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.parent.chmod(0o700)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", dir=path.parent
    )
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as temporary:
            json.dump(value, temporary, indent=2, sort_keys=True)
            temporary.write("\n")
        os.replace(temporary_name, path)
    except BaseException:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise


def require_cleanup_inventory(inventory: dict[str, Any], output: Path) -> None:
    """ECR is allowed while staging and must be gone after dev destroy."""
    if output.name == "21-inventory-after.json" and inventory["counts"]["ecr"]:
        inventory["runtime_resources_present"]["ecr"] = inventory["counts"]["ecr"]
        inventory["runtime_empty"] = False


def main() -> int:
    args = parse_args()
    try:
        output = validate_destination(args.run_id, args.output)
        require_preflight(args.run_id, args.commit)
        inventory = collect(args.region)
        require_cleanup_inventory(inventory, output)
        inventory["run_id"] = args.run_id
        inventory["source_commit"] = args.commit
        write_json(output, inventory)
    except InventoryError as error:
        print(f"inventory failed: {error}", file=os.sys.stderr)
        return 1

    if args.require_no_runtime and not inventory["runtime_empty"]:
        names = ", ".join(sorted(inventory["runtime_resources_present"]))
        print(f"runtime inventory is not empty: {names}", file=os.sys.stderr)
        return 1
    print(f"captured AWS inventory: {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
