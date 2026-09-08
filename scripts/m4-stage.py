#!/usr/bin/env python3
"""Stage and inspect immutable M4 images in Amazon ECR."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
TF_DIR = ROOT / "infra" / "terraform" / "envs" / "dev"
RUN_ID_RE = re.compile(r"^[0-9]{8}T[0-9]{6}Z$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
REPOSITORY_RE = re.compile(
    r"^(?P<registry>[0-9]{12}\.dkr\.ecr\.(?P<region>[a-z0-9-]+)"
    r"\.amazonaws\.com(?:\.cn)?)/mlp-dev/(?P<service>relay|sink)$"
)
SERVICES = ("relay", "sink")
LOCAL_IMAGES = {"relay": "relay:m4-aws", "sink": "sink:m4-aws"}
OUTPUT_NAME = "05-images.json"
PRICE_INPUT_NAME = "price-input.json"
PRICE_OUTPUT_NAME = "02-prices.md"
PRICE_DATA_NAME = "02-prices.json"
GO_NO_GO_NAME = "06-go-no-go.json"
MAXIMUM_HOURLY_USD = Decimal("1.25")
MAXIMUM_TOTAL_USD = Decimal("5.00")
PRICE_SOURCES = {
    "msk": "https://aws.amazon.com/msk/pricing/",
    "eks": "https://aws.amazon.com/eks/pricing/",
    "ec2": "https://aws.amazon.com/ec2/pricing/on-demand/",
    "vpc": "https://aws.amazon.com/vpc/pricing/",
    "rds": "https://aws.amazon.com/rds/postgresql/pricing/",
}
PRICE_RATE_NAMES = (
    "msk_serverless_cluster_hour",
    "msk_partition_hour",
    "eks_standard_cluster_hour",
    "t3_medium_hour",
    "nat_gateway_hour",
    "public_ipv4_hour",
    "rds_t4g_micro_hour",
    "rds_gp3_gb_month",
)


class StageError(RuntimeError):
    """The M4 staging contract was not met."""


class Runner:
    def result(
        self, arguments: list[str], *, input_bytes: bytes | None = None
    ) -> subprocess.CompletedProcess[bytes]:
        environment = os.environ.copy()
        environment["AWS_PAGER"] = ""
        result = subprocess.run(
            arguments,
            check=False,
            input=input_bytes,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=environment,
        )
        if result.returncode != 0:
            detail = result.stderr.decode("utf-8", errors="replace").strip()
            raise StageError(
                f"{' '.join(arguments)} failed: {detail or 'no error detail'}"
            )
        return result

    def run(self, arguments: list[str], *, input_bytes: bytes | None = None) -> None:
        self.result(arguments, input_bytes=input_bytes)

    def text(self, arguments: list[str]) -> str:
        return self.result(arguments).stdout.decode("utf-8").strip()

    def bytes(self, arguments: list[str]) -> bytes:
        return self.result(arguments).stdout

    def json(self, arguments: list[str]) -> Any:
        output = self.result(arguments).stdout
        try:
            return json.loads(output)
        except json.JSONDecodeError as error:
            raise StageError(f"{' '.join(arguments)} returned invalid JSON") from error


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="action", required=True)
    for action, help_text in (
        ("stage-images", "push missing immutable images and write evidence"),
        ("images", "inspect previously staged images and write evidence"),
    ):
        command = subparsers.add_parser(action, help=help_text)
        command.add_argument("--run-id", required=True)
        command.add_argument("--commit", required=True)
        command.add_argument("--region", required=True)
        command.add_argument("--output", required=True, type=Path)
    template = subparsers.add_parser(
        "price-template", help="write the private price-review input template"
    )
    template.add_argument("--run-id", required=True)
    template.add_argument("--commit", required=True)
    template.add_argument("--output", required=True, type=Path)
    prices = subparsers.add_parser(
        "prices", help="validate reviewed prices and write the price evidence"
    )
    prices.add_argument("--run-id", required=True)
    prices.add_argument("--commit", required=True)
    prices.add_argument("--region", required=True)
    prices.add_argument("--input", required=True, type=Path)
    prices.add_argument("--output", required=True, type=Path)
    release = subparsers.add_parser(
        "go-no-go", help="validate staged evidence and write the release packet"
    )
    release.add_argument("--run-id", required=True)
    release.add_argument("--commit", required=True)
    release.add_argument("--region", required=True)
    release.add_argument("--cleanup-owner", required=True)
    release.add_argument("--output", required=True, type=Path)
    verify = subparsers.add_parser(
        "verify-go-no-go", help="verify the release packet at the apply boundary"
    )
    verify.add_argument("--run-id", required=True)
    verify.add_argument("--commit", required=True)
    verify.add_argument("--region", required=True)
    verify.add_argument("--plan", required=True, type=Path)
    verify.add_argument("--summary", required=True, type=Path)
    verify.add_argument("--output", required=True, type=Path)
    return parser.parse_args()


def validate_run_id(run_id: str) -> None:
    if not RUN_ID_RE.fullmatch(run_id):
        raise StageError("run id must use UTC YYYYMMDDTHHMMSSZ")
    try:
        parsed = datetime.strptime(run_id, "%Y%m%dT%H%M%SZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError as error:
        raise StageError("run id is not a real UTC date and time") from error
    if parsed.strftime("%Y%m%dT%H%M%SZ") != run_id:
        raise StageError("run id is not a canonical UTC timestamp")


def ensure_private_run_path(
    run_id: str, output: Path, expected_name: str = OUTPUT_NAME
) -> Path:
    validate_run_id(run_id)
    expected_parent = ROOT / ".evidence" / "m4" / run_id
    current = expected_parent
    while current != ROOT:
        if current.is_symlink():
            raise StageError(f"evidence path contains a symlink: {current}")
        if ROOT not in current.parents:
            raise StageError("evidence path escapes the repository")
        current = current.parent
    destination = output.absolute()
    if (
        destination.parent != expected_parent.absolute()
        or destination.name != expected_name
    ):
        raise StageError(
            f"output must be {expected_name} directly under .evidence/m4/{run_id}"
        )
    if destination.is_symlink():
        raise StageError(f"evidence file must not be a symlink: {destination}")
    return destination


def require_preflight(run_id: str, commit: str) -> dict[str, Any]:
    validate_run_id(run_id)
    if not COMMIT_RE.fullmatch(commit):
        raise StageError("approved commit must be a full lowercase git SHA")
    path = ROOT / ".evidence" / "m4" / run_id / "00-preflight.json"
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise StageError(f"preflight receipt is missing: {path}") from error
    except json.JSONDecodeError as error:
        raise StageError(f"preflight receipt is invalid JSON: {path}") from error
    if not isinstance(value, dict):
        raise StageError("preflight receipt must contain an object")
    if value.get("schema_version") != 1 or value.get("run_id") != run_id:
        raise StageError("preflight receipt does not match the run id")
    if value.get("result") != "passed":
        raise StageError("preflight receipt did not pass")
    if value.get("commit") != commit:
        raise StageError("preflight receipt commit does not match the approved commit")
    return value


def require_clean_commit(commit: str, runner: Runner) -> None:
    head = runner.text(["git", "rev-parse", "HEAD"])
    if head != commit:
        raise StageError(f"HEAD is {head}, expected approved commit {commit}")
    dirty = runner.text(["git", "status", "--porcelain", "--untracked-files=all"])
    if dirty:
        raise StageError(f"worktree must be clean before image staging:\n{dirty}")


def repositories(region: str, runner: Runner) -> dict[str, str]:
    value = runner.json(
        [
            "terraform",
            f"-chdir={TF_DIR}",
            "output",
            "-json",
            "ecr_repository_urls",
        ]
    )
    if not isinstance(value, dict) or set(value) != set(SERVICES):
        raise StageError(
            "Terraform must return exactly the relay and sink repositories"
        )
    parsed: dict[str, str] = {}
    registry = None
    for service in SERVICES:
        repository = value.get(service)
        match = (
            REPOSITORY_RE.fullmatch(repository) if isinstance(repository, str) else None
        )
        if not match or match.group("service") != service:
            raise StageError(f"Terraform returned an invalid {service} repository")
        if match.group("region") != region:
            raise StageError(
                f"{service} repository region {match.group('region')} does not match {region}"
            )
        if registry is not None and registry != match.group("registry"):
            raise StageError("relay and sink repositories use different registries")
        registry = match.group("registry")
        parsed[service] = repository
    return parsed


def validate_repository_settings(
    expected: dict[str, str], value: Any
) -> dict[str, dict[str, Any]]:
    if not isinstance(value, dict) or not isinstance(value.get("repositories"), list):
        raise StageError("ECR returned an invalid repository list")
    by_name = {
        repository.get("repositoryName"): repository
        for repository in value["repositories"]
        if isinstance(repository, dict)
    }
    checked: dict[str, dict[str, Any]] = {}
    for service, repository_uri in expected.items():
        name = f"mlp-dev/{service}"
        repository = by_name.get(name)
        if not repository or repository.get("repositoryUri") != repository_uri:
            raise StageError(f"ECR repository {name} is missing or has the wrong URI")
        if repository.get("imageTagMutability") != "IMMUTABLE":
            raise StageError(f"ECR repository {name} does not enforce immutable tags")
        scanning = repository.get("imageScanningConfiguration")
        if not isinstance(scanning, dict) or scanning.get("scanOnPush") is not True:
            raise StageError(f"ECR repository {name} does not scan on push")
        checked[service] = {
            "repository": name,
            "uri": repository_uri,
            "tag_mutability": "IMMUTABLE",
            "scan_on_push": True,
        }
    return checked


def repository_settings(
    expected: dict[str, str], region: str, runner: Runner
) -> dict[str, dict[str, Any]]:
    value = runner.json(
        [
            "aws",
            "ecr",
            "describe-repositories",
            "--repository-names",
            *(f"mlp-dev/{service}" for service in SERVICES),
            "--region",
            region,
            "--output",
            "json",
        ]
    )
    return validate_repository_settings(expected, value)


def validate_image_metadata(value: Any, image: str, commit: str) -> dict[str, str]:
    if not isinstance(value, dict):
        raise StageError(f"docker returned invalid metadata for {image}")
    labels = value.get("Config", {}).get("Labels")
    revision = (
        labels.get("org.opencontainers.image.revision")
        if isinstance(labels, dict)
        else None
    )
    architecture = value.get("Architecture")
    operating_system = value.get("Os")
    image_id = value.get("Id")
    if revision != commit:
        raise StageError(f"{image} was built from {revision!r}, expected {commit}")
    if operating_system != "linux" or architecture != "amd64":
        raise StageError(
            f"{image} uses {operating_system}/{architecture}; expected linux/amd64"
        )
    if not isinstance(image_id, str) or not DIGEST_RE.fullmatch(image_id):
        raise StageError(f"{image} has an invalid image id: {image_id}")
    return {
        "id": image_id,
        "revision": revision,
        "platform": "linux/amd64",
    }


def inspect_image(image: str, commit: str, runner: Runner) -> dict[str, str]:
    value = runner.json(
        [
            "docker",
            "image",
            "inspect",
            "--platform",
            "linux/amd64",
            "--format",
            "{{json .}}",
            image,
        ]
    )
    return validate_image_metadata(value, image, commit)


def existing_digest(
    repository: str, commit: str, region: str, runner: Runner
) -> str | None:
    value = runner.json(
        [
            "aws",
            "ecr",
            "list-images",
            "--repository-name",
            repository,
            "--filter",
            "tagStatus=TAGGED",
            "--region",
            region,
            "--output",
            "json",
        ]
    )
    if not isinstance(value, dict) or not isinstance(value.get("imageIds"), list):
        raise StageError(f"ECR returned an invalid image list for {repository}")
    matches = [
        image.get("imageDigest")
        for image in value["imageIds"]
        if isinstance(image, dict) and image.get("imageTag") == commit
    ]
    if len(matches) > 1:
        raise StageError(f"ECR returned duplicate entries for {repository}:{commit}")
    if not matches:
        return None
    digest = matches[0]
    if not isinstance(digest, str) or not DIGEST_RE.fullmatch(digest):
        raise StageError(f"ECR returned an invalid digest for {repository}:{commit}")
    return digest


def described_digest(repository: str, commit: str, region: str, runner: Runner) -> str:
    value = runner.json(
        [
            "aws",
            "ecr",
            "describe-images",
            "--repository-name",
            repository,
            "--image-ids",
            f"imageTag={commit}",
            "--region",
            region,
            "--output",
            "json",
        ]
    )
    details = value.get("imageDetails") if isinstance(value, dict) else None
    if not isinstance(details, list) or len(details) != 1:
        raise StageError(f"ECR did not return one image for {repository}:{commit}")
    detail = details[0]
    digest = detail.get("imageDigest") if isinstance(detail, dict) else None
    tags = detail.get("imageTags") if isinstance(detail, dict) else None
    if not isinstance(digest, str) or not DIGEST_RE.fullmatch(digest):
        raise StageError(f"ECR returned an invalid digest for {repository}:{commit}")
    if not isinstance(tags, list) or commit not in tags:
        raise StageError(f"ECR image {repository}:{commit} lost its immutable tag")
    return digest


def registry_for(repositories_by_service: dict[str, str]) -> str:
    registries = {
        repository.split("/", maxsplit=1)[0]
        for repository in repositories_by_service.values()
    }
    if len(registries) != 1:
        raise StageError("relay and sink repositories use different registries")
    return registries.pop()


def login(registry: str, region: str, runner: Runner) -> None:
    password = runner.bytes(["aws", "ecr", "get-login-password", "--region", region])
    if not password.strip():
        raise StageError("ECR returned an empty login password")
    runner.run(
        ["docker", "login", "--username", "AWS", "--password-stdin", registry],
        input_bytes=password,
    )


def stage_one(
    service: str,
    repository_uri: str,
    commit: str,
    region: str,
    push_missing: bool,
    runner: Runner,
) -> dict[str, Any]:
    repository = f"mlp-dev/{service}"
    digest = existing_digest(repository, commit, region, runner)
    pushed = False
    if digest is None:
        if not push_missing:
            raise StageError(f"ECR image is missing: {repository}:{commit}")
        local_image = LOCAL_IMAGES[service]
        local = inspect_image(local_image, commit, runner)
        tagged = f"{repository_uri}:{commit}"
        runner.run(["docker", "image", "tag", local_image, tagged])
        runner.run(["docker", "image", "push", tagged])
        pushed = True
        digest = described_digest(repository, commit, region, runner)
        remote = local
    else:
        confirmed = described_digest(repository, commit, region, runner)
        if confirmed != digest:
            raise StageError(
                f"ECR digest changed while inspecting {repository}:{commit}"
            )
        reference = f"{repository_uri}@{digest}"
        runner.run(["docker", "image", "pull", "--platform", "linux/amd64", reference])
        remote = inspect_image(reference, commit, runner)

    return {
        "repository": repository,
        "tag": commit,
        "digest": digest,
        "reference": f"{repository_uri}@{digest}",
        "platform": remote["platform"],
        "image_id": remote["id"],
        "revision": remote["revision"],
        "pushed": pushed,
    }


def capture_images(
    commit: str, region: str, push_missing: bool, runner: Runner
) -> dict[str, Any]:
    require_clean_commit(commit, runner)
    repository_urls = repositories(region, runner)
    settings = repository_settings(repository_urls, region, runner)
    login(registry_for(repository_urls), region, runner)
    images = {
        service: stage_one(
            service,
            repository_urls[service],
            commit,
            region,
            push_missing,
            runner,
        )
        for service in SERVICES
    }
    if images["relay"]["digest"] == images["sink"]["digest"]:
        raise StageError("relay and sink unexpectedly have the same manifest digest")
    return {
        "schema_version": 1,
        "captured_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source_commit": commit,
        "region": region,
        "repository_settings": settings,
        "images": images,
        "gate": {"passed": True, "failures": []},
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


def write_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.parent.chmod(0o700)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", dir=path.parent
    )
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as temporary:
            temporary.write(value)
        os.replace(temporary_name, path)
    except BaseException:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise


def read_json(path: Path, description: str) -> dict[str, Any]:
    if path.is_symlink():
        raise StageError(f"{description} must not be a symlink: {path}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise StageError(f"{description} is missing: {path}") from error
    except json.JSONDecodeError as error:
        raise StageError(f"{description} is invalid JSON: {path}") from error
    if not isinstance(value, dict):
        raise StageError(f"{description} must contain a JSON object")
    return value


def price_template(run_id: str, commit: str) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "run_id": run_id,
        "source_commit": commit,
        "region": "us-east-1",
        "currency": "USD",
        "confirmed": False,
        "checked_at": "",
        "checked_by": "",
        "sources": dict(PRICE_SOURCES),
        "rates": {name: "" for name in PRICE_RATE_NAMES},
    }


def decimal_rate(value: Any, name: str) -> Decimal:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", value):
        raise StageError(f"price {name} must be a non-negative decimal string")
    try:
        parsed = Decimal(value)
    except InvalidOperation as error:
        raise StageError(f"price {name} is invalid") from error
    if not parsed.is_finite():
        raise StageError(f"price {name} must be finite")
    return parsed


def parse_recent_utc(
    value: Any, description: str, now: datetime, maximum_age: timedelta
) -> datetime:
    if not isinstance(value, str):
        raise StageError(f"{description} must use UTC YYYY-MM-DDTHH:MM:SSZ")
    try:
        checked = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError as error:
        raise StageError(f"{description} must be a real UTC timestamp") from error
    if checked > now + timedelta(minutes=5):
        raise StageError(f"{description} is in the future")
    if now - checked > maximum_age:
        raise StageError(f"{description} is older than 24 hours")
    return checked


def parse_checked_at(value: Any, now: datetime) -> datetime:
    return parse_recent_utc(value, "price check time", now, timedelta(hours=24))


def validate_price_input(
    value: dict[str, Any],
    run_id: str,
    commit: str,
    region: str,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    if value.get("schema_version") != 1:
        raise StageError("price input schema_version must be 1")
    if value.get("run_id") != run_id or value.get("source_commit") != commit:
        raise StageError("price input does not match the run and approved commit")
    if value.get("region") != region or region != "us-east-1":
        raise StageError("price input must use us-east-1")
    if value.get("currency") != "USD":
        raise StageError("price input currency must be USD")
    if value.get("confirmed") is not True:
        raise StageError("price input must be confirmed after checking every source")
    reviewer = value.get("checked_by")
    if not isinstance(reviewer, str) or not reviewer.strip() or "\n" in reviewer:
        raise StageError("checked_by must name the price reviewer")
    checked = parse_checked_at(
        value.get("checked_at"), now or datetime.now(timezone.utc)
    )
    if value.get("sources") != PRICE_SOURCES:
        raise StageError("price input must retain the fixed primary-source URLs")
    rates = value.get("rates")
    if not isinstance(rates, dict) or set(rates) != set(PRICE_RATE_NAMES):
        raise StageError(
            "price input must contain every fixed rate and no unknown rate"
        )
    parsed_rates = {name: decimal_rate(rates[name], name) for name in rates}

    components = [
        (
            "MSK Serverless cluster",
            "msk_serverless_cluster_hour",
            Decimal("1"),
            Decimal("1"),
            "cluster-hour",
            "msk",
        ),
        (
            "MSK partitions",
            "msk_partition_hour",
            Decimal("13"),
            Decimal("1"),
            "partition-hour",
            "msk",
        ),
        (
            "EKS standard-support control plane",
            "eks_standard_cluster_hour",
            Decimal("1"),
            Decimal("1"),
            "cluster-hour",
            "eks",
        ),
        (
            "t3.medium on-demand upper bound",
            "t3_medium_hour",
            Decimal("2"),
            Decimal("1"),
            "instance-hour",
            "ec2",
        ),
        (
            "NAT gateway",
            "nat_gateway_hour",
            Decimal("1"),
            Decimal("1"),
            "gateway-hour",
            "vpc",
        ),
        (
            "NAT public IPv4 address",
            "public_ipv4_hour",
            Decimal("1"),
            Decimal("1"),
            "address-hour",
            "vpc",
        ),
        (
            "RDS db.t4g.micro",
            "rds_t4g_micro_hour",
            Decimal("1"),
            Decimal("1"),
            "instance-hour",
            "rds",
        ),
        (
            "RDS gp3 storage",
            "rds_gp3_gb_month",
            Decimal("20"),
            Decimal("730"),
            "GB-month",
            "rds",
        ),
    ]
    items = []
    for label, rate_name, quantity, divisor, unit, source in components:
        hourly = parsed_rates[rate_name] * quantity / divisor
        items.append(
            {
                "item": label,
                "rate_name": rate_name,
                "rate_usd": str(parsed_rates[rate_name]),
                "rate_unit": unit,
                "quantity": str(quantity),
                "hourly_divisor": str(divisor),
                "hourly_usd": str(hourly),
                "source": PRICE_SOURCES[source],
            }
        )
    total = sum((Decimal(item["hourly_usd"]) for item in items), Decimal("0"))
    if total > MAXIMUM_HOURLY_USD:
        raise StageError(
            f"current price total {total} exceeds the {MAXIMUM_HOURLY_USD} hourly limit"
        )
    return {
        "schema_version": 1,
        "run_id": run_id,
        "source_commit": commit,
        "region": region,
        "currency": "USD",
        "checked_at": checked.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "checked_by": reviewer.strip(),
        "sources": PRICE_SOURCES,
        "items": items,
        "total_hourly_usd": str(total),
        "maximum_hourly_usd": str(MAXIMUM_HOURLY_USD),
        "maximum_total_usd": str(MAXIMUM_TOTAL_USD),
        "gate": {"passed": True, "failures": []},
    }


def price_markdown(receipt: dict[str, Any]) -> str:
    lines = [
        "# M4 price check",
        "",
        f"Checked at: `{receipt['checked_at']}`",
        "",
        f"Checked by: `{receipt['checked_by']}`",
        "",
        f"Region: `{receipt['region']}`",
        "",
        "| Item | Rate | Quantity | Hourly USD | Source |",
        "|---|---:|---:|---:|---|",
    ]
    for item in receipt["items"]:
        quantity = item["quantity"]
        if item["hourly_divisor"] != "1":
            quantity += f" / {item['hourly_divisor']} hours"
        lines.append(
            f"| {item['item']} | ${item['rate_usd']}/{item['rate_unit']} | "
            f"{quantity} | ${Decimal(item['hourly_usd']):.6f} | "
            f"[AWS]({item['source']}) |"
        )
    lines.extend(
        [
            "",
            f"Total: **${Decimal(receipt['total_hourly_usd']):.4f}/hour**",
            "",
            f"Gate: **PASS**, below the ${receipt['maximum_hourly_usd']}/hour limit.",
            "",
        ]
    )
    return "\n".join(lines)


def revalidate_price_receipt(
    receipt: dict[str, Any],
    run_id: str,
    commit: str,
    region: str,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    items = receipt.get("items")
    if not isinstance(items, list):
        raise StageError("price evidence has no itemized rates")
    rates: dict[str, Any] = {}
    for item in items:
        if not isinstance(item, dict) or not isinstance(item.get("rate_name"), str):
            raise StageError("price evidence contains an invalid item")
        name = item["rate_name"]
        if name in rates:
            raise StageError(f"price evidence repeats {name}")
        rates[name] = item.get("rate_usd")
    source = {
        "schema_version": receipt.get("schema_version"),
        "run_id": receipt.get("run_id"),
        "source_commit": receipt.get("source_commit"),
        "region": receipt.get("region"),
        "currency": receipt.get("currency"),
        "confirmed": True,
        "checked_at": receipt.get("checked_at"),
        "checked_by": receipt.get("checked_by"),
        "sources": receipt.get("sources"),
        "rates": rates,
    }
    expected = validate_price_input(source, run_id, commit, region, now=now)
    if expected != receipt:
        raise StageError("price evidence does not match the recomputed fixed topology")
    return expected


def hash_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def require_bound(
    value: dict[str, Any], description: str, run_id: str, commit: str, region: str
) -> None:
    if value.get("run_id") != run_id:
        raise StageError(f"{description} does not match the run id")
    if value.get("source_commit", value.get("commit")) != commit:
        raise StageError(f"{description} does not match the approved commit")
    if "region" in value and value.get("region") != region:
        raise StageError(f"{description} does not match the region")


def require_passed_gate(value: dict[str, Any], description: str) -> None:
    gate = value.get("gate")
    if not isinstance(gate, dict) or gate.get("passed") is not True:
        raise StageError(f"{description} gate did not pass")


def decimal_value(value: Any, description: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise StageError(f"{description} is not numeric")
    try:
        result = Decimal(str(value))
    except InvalidOperation as error:
        raise StageError(f"{description} is not numeric") from error
    if not result.is_finite():
        raise StageError(f"{description} is not finite")
    return result


def build_go_no_go(
    run_id: str,
    commit: str,
    region: str,
    cleanup_owner: str,
    runner: Runner,
) -> dict[str, Any]:
    if not cleanup_owner.strip() or "\n" in cleanup_owner:
        raise StageError("cleanup owner must name the executing operator")
    require_clean_commit(commit, runner)
    raw = ROOT / ".evidence" / "m4" / run_id
    paths = {
        "preflight": raw / "00-preflight.json",
        "identity": raw / "01-identity.txt",
        "prices_markdown": raw / PRICE_OUTPUT_NAME,
        "prices": raw / PRICE_DATA_NAME,
        "plan": raw / "03-plan-summary.json",
        "inventory": raw / "04-inventory-before.json",
        "images": raw / OUTPUT_NAME,
        "capture_plan": raw / "capture-plan.json",
    }
    preflight = require_preflight(run_id, commit)
    identity = read_json(paths["identity"], "identity evidence")
    prices = revalidate_price_receipt(
        read_json(paths["prices"], "price evidence"),
        run_id,
        commit,
        region,
    )
    plan = read_json(paths["plan"], "plan summary")
    inventory = read_json(paths["inventory"], "inventory evidence")
    images = read_json(paths["images"], "image evidence")
    capture_plan = read_json(paths["capture_plan"], "capture plan")
    if paths["prices_markdown"].is_symlink():
        raise StageError("price Markdown evidence must not be a symlink")
    try:
        price_text = paths["prices_markdown"].read_text(encoding="utf-8")
    except FileNotFoundError as error:
        raise StageError("price Markdown evidence is missing") from error
    if price_text != price_markdown(prices):
        raise StageError("price Markdown does not match the checked price data")

    for value, description in (
        (identity, "identity evidence"),
        (prices, "price evidence"),
        (plan, "plan summary"),
        (inventory, "inventory evidence"),
        (images, "image evidence"),
        (capture_plan, "capture plan"),
    ):
        require_bound(value, description, run_id, commit, region)

    parse_recent_utc(
        inventory.get("captured_at"),
        "inventory capture time",
        datetime.now(timezone.utc),
        timedelta(hours=24),
    )
    parse_recent_utc(
        plan.get("captured_at"),
        "plan capture time",
        datetime.now(timezone.utc),
        timedelta(hours=24),
    )

    for value, description in (
        (identity, "identity evidence"),
        (prices, "price evidence"),
        (plan, "plan summary"),
        (images, "image evidence"),
    ):
        require_passed_gate(value, description)

    repository = identity.get("repository")
    aws_identity = identity.get("aws")
    state_backend = identity.get("backend")
    eks = identity.get("eks")
    budget = identity.get("budget")
    quotas = identity.get("quotas")
    availability = identity.get("availability")
    if (
        not isinstance(repository, dict)
        or repository.get("name_with_owner") != "lilabrooks/my-local-platform"
    ):
        raise StageError("identity evidence names the wrong GitHub repository")
    if (
        not isinstance(aws_identity, dict)
        or aws_identity.get("account_matches_profile") is not True
    ):
        raise StageError("AWS caller identity does not match the selected profile")
    if (
        not isinstance(aws_identity.get("profile"), str)
        or not aws_identity["profile"].strip()
    ):
        raise StageError("AWS identity does not name the selected profile")
    account_id = aws_identity.get("account_id")
    if not isinstance(account_id, str) or not re.fullmatch(r"[0-9]{12}", account_id):
        raise StageError("AWS identity has no valid account id")
    parse_recent_utc(
        identity.get("captured_at"),
        "account capture time",
        datetime.now(timezone.utc),
        timedelta(hours=24),
    )
    if not isinstance(state_backend, dict) or state_backend != {
        "bucket": f"mlp-tfstate-{account_id}",
        "exists": True,
        "versioning": "Enabled",
        "encryption": "AES256",
        "public_access_blocked": True,
    }:
        raise StageError("state backend controls did not pass")
    if not isinstance(eks, dict) or eks.get("standard_support") is not True:
        raise StageError("EKS standard-support evidence did not pass")
    if not isinstance(budget, dict) or budget.get("active") is not True:
        raise StageError("budget evidence did not pass")
    if budget.get("has_notification_subscriber") is not True:
        raise StageError("budget evidence has no notification subscriber")
    budget_limit = decimal_value(budget.get("limit_usd"), "budget limit")
    if budget_limit <= 0:
        raise StageError("budget limit must be positive")
    if budget_limit > MAXIMUM_TOTAL_USD:
        raise StageError("budget limit exceeds the approved maximum")
    if not isinstance(quotas, dict):
        raise StageError("quota evidence is missing")
    require_passed_gate(quotas, "quota evidence")
    if not isinstance(availability, dict):
        raise StageError("regional availability evidence is missing")
    require_passed_gate(availability, "regional availability evidence")

    if inventory.get("runtime_empty") is not True:
        raise StageError("pre-apply runtime inventory is not empty")
    counts = inventory.get("counts")
    if not isinstance(counts, dict) or counts.get("ecr") != 2:
        raise StageError("pre-apply inventory must contain both ECR repositories")

    shape = plan.get("shape")
    if not isinstance(shape, dict):
        raise StageError("plan summary has no runtime shape")
    for flag in ("hourly_enabled", "enable_eks", "enable_msk", "enable_rds"):
        if shape.get(flag) is not True:
            raise StageError(f"plan summary does not enable {flag}")
    if shape.get("region") != region:
        raise StageError("plan shape uses the wrong region")
    if shape.get("eks") != {
        "kubernetes_version": "1.35",
        "node_capacity_type": "SPOT",
        "node_desired": 2,
        "node_maximum": 3,
    }:
        raise StageError("plan summary has the wrong EKS shape")
    if shape.get("kafka") != {
        "delivery_topic": "mlp.relay.deliveries",
        "delivery_partitions": 12,
        "dead_letter_topic": "mlp.relay.deliveries.dlq",
        "dead_letter_partitions": 1,
        "total_partitions": 13,
    }:
        raise StageError("plan summary has the wrong Kafka shape")
    expected_counts = {
        "aws_db_instance": 1,
        "aws_eks_cluster": 1,
        "aws_eks_node_group": 1,
        "aws_msk_serverless_cluster": 1,
        "aws_nat_gateway": 1,
    }
    if plan.get("planned_hourly_resource_counts") != expected_counts:
        raise StageError("plan summary has the wrong hourly resource counts")
    if plan.get("created_hourly_resource_counts") != expected_counts:
        raise StageError("plan summary does not create the fixed hourly topology")
    modelled = decimal_value(shape.get("expected_hourly_usd"), "plan hourly cost")
    maximum = decimal_value(shape.get("maximum_hourly_usd"), "plan hourly limit")
    current = decimal_value(prices.get("total_hourly_usd"), "current hourly cost")
    if maximum != MAXIMUM_HOURLY_USD:
        raise StageError("plan hourly limit changed from the approved contract")
    if abs(modelled - current) > Decimal("0.0001"):
        raise StageError("Terraform and current price arithmetic differ")

    image_values = images.get("images")
    if not isinstance(image_values, dict) or set(image_values) != set(SERVICES):
        raise StageError("image evidence must contain relay and sink")
    references: dict[str, str] = {}
    digests = set()
    registries = set()
    for service in SERVICES:
        image = image_values.get(service)
        if not isinstance(image, dict) or image.get("revision") != commit:
            raise StageError(f"{service} image does not match the approved commit")
        if image.get("platform") != "linux/amd64":
            raise StageError(f"{service} image does not use linux/amd64")
        digest = image.get("digest")
        reference = image.get("reference")
        if not isinstance(digest, str) or not DIGEST_RE.fullmatch(digest):
            raise StageError(f"{service} image digest is invalid")
        if not isinstance(reference, str) or not reference.endswith(f"@{digest}"):
            raise StageError(f"{service} image reference is not pinned by digest")
        repository_uri = reference.removesuffix(f"@{digest}")
        match = REPOSITORY_RE.fullmatch(repository_uri)
        if not match or match.group("service") != service:
            raise StageError(f"{service} image reference names the wrong repository")
        if match.group("region") != region:
            raise StageError(f"{service} image reference uses the wrong region")
        registries.add(match.group("registry"))
        digests.add(digest)
        references[service] = reference
    if len(digests) != 2:
        raise StageError("relay and sink must use different image digests")
    if len(registries) != 1:
        raise StageError("relay and sink image references use different registries")
    if registries != {f"{account_id}.dkr.ecr.{region}.amazonaws.com"}:
        raise StageError("image registry does not match the verified AWS account")
    settings = images.get("repository_settings")
    if not isinstance(settings, dict) or set(settings) != set(SERVICES):
        raise StageError("image evidence has no repository settings")
    for service in SERVICES:
        setting = settings.get(service)
        if not isinstance(setting, dict) or (
            setting.get("repository") != f"mlp-dev/{service}"
            or setting.get("uri") != references[service].split("@", maxsplit=1)[0]
            or setting.get("tag_mutability") != "IMMUTABLE"
            or setting.get("scan_on_push") is not True
        ):
            raise StageError(f"{service} repository controls did not pass")

    paid_window = capture_plan.get("paid_window")
    failure = capture_plan.get("failure_transition")
    captures = capture_plan.get("captures")
    if not isinstance(paid_window, dict) or (
        paid_window.get("evidence_deadline_minutes") != 150
        or paid_window.get("hard_deadline_minutes") != 180
        or paid_window.get("destroy_starts_after_success_or_failure") is not True
    ):
        raise StageError("capture plan changed the paid-window limits")
    if not isinstance(failure, dict) or failure.get("next_phase") != "destroy_first":
        raise StageError("capture plan does not enter destroy first on failure")
    if not isinstance(captures, list) or not captures:
        raise StageError("capture plan has no ordered captures")
    capture_order = []
    previous = -1
    for capture in captures:
        if not isinstance(capture, dict) or not isinstance(capture.get("order"), int):
            raise StageError("capture plan contains an invalid step")
        if capture["order"] <= previous:
            raise StageError("capture plan order is not strictly increasing")
        previous = capture["order"]
        capture_order.append(
            {
                "order": capture["order"],
                "phase": capture.get("phase"),
                "output": capture.get("output"),
            }
        )

    return {
        "schema_version": 1,
        "run_id": run_id,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "decision": "go",
        "source_commit": commit,
        "region": region,
        "repository": repository["name_with_owner"],
        "aws_profile": aws_identity.get("profile"),
        "image_references": references,
        "expected_topology": shape,
        "plan": {
            "sha256": plan.get("plan_sha256"),
            "planned_resource_count": plan.get("planned_resource_count"),
            "created_resource_count": plan.get("created_resource_count"),
        },
        "pricing": {
            "checked_at": prices.get("checked_at"),
            "total_hourly_usd": prices.get("total_hourly_usd"),
            "sources": prices.get("sources"),
        },
        "limits": {
            "maximum_hourly_usd": str(MAXIMUM_HOURLY_USD),
            "maximum_total_usd": str(MAXIMUM_TOTAL_USD),
            "evidence_deadline_minutes": 150,
            "hard_deadline_minutes": 180,
        },
        "capture_order": capture_order,
        "abort_command": "make aws-down",
        "cleanup_owner": cleanup_owner.strip(),
        "input_sha256": {name: hash_file(path) for name, path in paths.items()},
        "preflight_result": preflight["result"],
        "gate": {"passed": True, "failures": []},
    }


def verify_go_no_go(
    run_id: str,
    commit: str,
    region: str,
    packet_path: Path,
    plan_path: Path,
    summary_path: Path,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    packet = read_json(packet_path, "go/no-go packet")
    require_bound(packet, "go/no-go packet", run_id, commit, region)
    require_passed_gate(packet, "go/no-go packet")
    if packet.get("schema_version") != 1 or packet.get("decision") != "go":
        raise StageError("go/no-go packet has no GO decision")
    parse_recent_utc(
        packet.get("generated_at"),
        "go/no-go decision time",
        now or datetime.now(timezone.utc),
        timedelta(hours=24),
    )
    plan = packet.get("plan")
    if not isinstance(plan, dict) or plan.get("sha256") != hash_file(plan_path):
        raise StageError("go/no-go packet does not match the reviewed plan")
    inputs = packet.get("input_sha256")
    if not isinstance(inputs, dict) or inputs.get("plan") != hash_file(summary_path):
        raise StageError("go/no-go packet does not match the reviewed summary")
    cleanup_owner = packet.get("cleanup_owner")
    if not isinstance(cleanup_owner, str) or not cleanup_owner.strip():
        raise StageError("go/no-go packet has no cleanup owner")
    if packet.get("abort_command") != "make aws-down":
        raise StageError("go/no-go packet has the wrong abort command")
    return packet


def main() -> int:
    args = parse_args()
    try:
        require_preflight(args.run_id, args.commit)
        if args.action == "price-template":
            output = ensure_private_run_path(args.run_id, args.output, PRICE_INPUT_NAME)
            if output.exists():
                raise StageError(f"price input already exists: {output}")
            write_json(output, price_template(args.run_id, args.commit))
            result = f"wrote price-review template: {output}"
        elif args.action == "prices":
            input_path = ensure_private_run_path(
                args.run_id, args.input, PRICE_INPUT_NAME
            )
            output = ensure_private_run_path(
                args.run_id, args.output, PRICE_OUTPUT_NAME
            )
            data_output = ensure_private_run_path(
                args.run_id, output.with_suffix(".json"), PRICE_DATA_NAME
            )
            receipt = validate_price_input(
                read_json(input_path, "price input"),
                args.run_id,
                args.commit,
                args.region,
            )
            write_text(output, price_markdown(receipt))
            write_json(data_output, receipt)
            result = f"captured current price evidence: {output}"
        elif args.action == "go-no-go":
            output = ensure_private_run_path(args.run_id, args.output, GO_NO_GO_NAME)
            receipt = build_go_no_go(
                args.run_id,
                args.commit,
                args.region,
                args.cleanup_owner,
                Runner(),
            )
            write_json(output, receipt)
            result = f"wrote GO packet: {output}"
        elif args.action == "verify-go-no-go":
            output = ensure_private_run_path(args.run_id, args.output, GO_NO_GO_NAME)
            verify_go_no_go(
                args.run_id,
                args.commit,
                args.region,
                output,
                args.plan,
                args.summary,
            )
            result = f"verified GO packet: {output}"
        else:
            output = ensure_private_run_path(args.run_id, args.output)
            receipt = capture_images(
                args.commit,
                args.region,
                args.action == "stage-images",
                Runner(),
            )
            receipt["run_id"] = args.run_id
            write_json(output, receipt)
            result = f"captured immutable ECR images: {output}"
    except StageError as error:
        print(f"M4 staging failed: {error}", file=sys.stderr)
        return 1
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
