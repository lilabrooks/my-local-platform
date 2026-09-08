#!/usr/bin/env python3
"""Capture the read-only account gates for M4 AWS staging."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
RUN_ID_RE = re.compile(r"^[0-9]{8}T[0-9]{6}Z$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
ACCOUNT_RE = re.compile(r"^[0-9]{12}$")
OUTPUT_NAME = "01-identity.txt"
EXPECTED_REPOSITORY = "lilabrooks/my-local-platform"
EXPECTED_REGION = "us-east-1"
EKS_VERSION = "1.35"
BUDGET_NAME = "mlp-dev-live-runtime"
FIXED_AZS = ("us-east-1a", "us-east-1b")
MSK_SERVERLESS_REGIONS = frozenset(
    {
        "ap-northeast-1",
        "ap-northeast-2",
        "ap-south-1",
        "ap-southeast-1",
        "ap-southeast-2",
        "ca-central-1",
        "eu-central-1",
        "eu-north-1",
        "eu-west-1",
        "eu-west-2",
        "eu-west-3",
        "us-east-1",
        "us-east-2",
        "us-west-2",
    }
)
EC2_QUOTAS = {
    # Three t3.medium nodes at the plan's fixed maximum, two vCPUs each.
    "spot_standard_vcpu": ("L-34B43A08", Decimal("6")),
    "elastic_ip": ("L-0263D0A3", Decimal("1")),
}
SOURCES = {
    "eks_quotas": "https://docs.aws.amazon.com/eks/latest/userguide/service-quotas.html",
    "eks_versions": "https://docs.aws.amazon.com/eks/latest/userguide/kubernetes-versions.html",
    "ec2_spot_quotas": "https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-spot-limits.html",
    "msk_serverless": "https://docs.aws.amazon.com/msk/latest/developerguide/serverless.html",
    "msk_quotas": "https://docs.aws.amazon.com/msk/latest/developerguide/limits.html",
    "rds_quotas": "https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/CHAP_Limits.html",
    "rds_offerings": "https://docs.aws.amazon.com/cli/latest/reference/rds/describe-orderable-db-instance-options.html",
    "regional_parameters": "https://docs.aws.amazon.com/systems-manager/latest/userguide/parameter-store-public-parameters-global-infrastructure.html",
    "ec2_offerings": "https://docs.aws.amazon.com/cli/latest/reference/ec2/describe-instance-type-offerings.html",
    "vpc_quotas": "https://docs.aws.amazon.com/vpc/latest/userguide/amazon-vpc-limits.html",
}


class AccountError(RuntimeError):
    """The M4 account gate could not be proved."""


class Runner:
    def result(self, arguments: list[str]) -> subprocess.CompletedProcess[bytes]:
        environment = os.environ.copy()
        environment["AWS_PAGER"] = ""
        result = subprocess.run(
            arguments,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=environment,
        )
        if result.returncode != 0:
            detail = result.stderr.decode("utf-8", errors="replace").strip()
            raise AccountError(
                f"{' '.join(arguments)} failed: {detail or 'no error detail'}"
            )
        return result

    def run(self, arguments: list[str]) -> None:
        self.result(arguments)

    def text(self, arguments: list[str]) -> str:
        return self.result(arguments).stdout.decode("utf-8").strip()

    def json(self, arguments: list[str]) -> Any:
        output = self.result(arguments).stdout
        try:
            return json.loads(output)
        except json.JSONDecodeError as error:
            raise AccountError(
                f"{' '.join(arguments)} returned invalid JSON"
            ) from error


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args()


def validate_run_id(run_id: str) -> None:
    if not RUN_ID_RE.fullmatch(run_id):
        raise AccountError("run id must use UTC YYYYMMDDTHHMMSSZ")
    try:
        parsed = datetime.strptime(run_id, "%Y%m%dT%H%M%SZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError as error:
        raise AccountError("run id is not a real UTC date and time") from error
    if parsed.strftime("%Y%m%dT%H%M%SZ") != run_id:
        raise AccountError("run id is not a canonical UTC timestamp")


def validate_destination(run_id: str, output: Path) -> Path:
    validate_run_id(run_id)
    expected_parent = ROOT / ".evidence" / "m4" / run_id
    current = expected_parent
    while current != ROOT:
        if current.is_symlink():
            raise AccountError(f"evidence path contains a symlink: {current}")
        if ROOT not in current.parents:
            raise AccountError("evidence path escapes the repository")
        current = current.parent
    destination = output.absolute()
    if (
        destination.parent != expected_parent.absolute()
        or destination.name != OUTPUT_NAME
    ):
        raise AccountError(
            f"output must be {OUTPUT_NAME} directly under .evidence/m4/{run_id}"
        )
    if destination.is_symlink():
        raise AccountError(f"evidence file must not be a symlink: {destination}")
    return destination


def require_preflight(run_id: str, commit: str) -> None:
    if not COMMIT_RE.fullmatch(commit):
        raise AccountError("approved commit must be a full lowercase git SHA")
    path = ROOT / ".evidence" / "m4" / run_id / "00-preflight.json"
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise AccountError(f"preflight receipt is missing: {path}") from error
    except json.JSONDecodeError as error:
        raise AccountError(f"preflight receipt is invalid JSON: {path}") from error
    if (
        not isinstance(value, dict)
        or value.get("schema_version") != 1
        or value.get("run_id") != run_id
        or value.get("commit") != commit
        or value.get("result") != "passed"
    ):
        raise AccountError("preflight receipt is not a pass for this run and commit")


def require_clean_commit(commit: str, runner: Runner) -> None:
    head = runner.text(["git", "rev-parse", "HEAD"])
    if head != commit:
        raise AccountError(f"HEAD is {head}, expected approved commit {commit}")
    dirty = runner.text(["git", "status", "--porcelain", "--untracked-files=all"])
    if dirty:
        raise AccountError(f"worktree must be clean before account capture:\n{dirty}")


def object_value(value: Any, description: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise AccountError(f"{description} returned a non-object")
    return value


def list_value(value: Any, key: str, description: str) -> list[Any]:
    obj = object_value(value, description)
    result = obj.get(key)
    if not isinstance(result, list):
        raise AccountError(f"{description} returned an invalid {key} list")
    return result


def number(value: Any, description: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise AccountError(f"{description} is not numeric")
    try:
        parsed = Decimal(str(value))
    except InvalidOperation as error:
        raise AccountError(f"{description} is not numeric") from error
    if not parsed.is_finite() or parsed < 0:
        raise AccountError(f"{description} must be a non-negative finite number")
    return parsed


def decimal_text(value: Decimal) -> str:
    return format(value, "f")


def repository_identity(runner: Runner) -> dict[str, Any]:
    value = object_value(
        runner.json(["gh", "repo", "view", "--json", "nameWithOwner,url"]),
        "GitHub repository identity",
    )
    if value.get("nameWithOwner") != EXPECTED_REPOSITORY:
        raise AccountError("GitHub CLI selected the wrong repository")
    url = value.get("url")
    if not isinstance(url, str) or not url.startswith("https://github.com/"):
        raise AccountError("GitHub repository identity returned an invalid URL")
    return {"name_with_owner": value["nameWithOwner"], "url": url}


def aws_identity(profile: str, runner: Runner) -> dict[str, Any]:
    caller = object_value(
        runner.json(["aws", "sts", "get-caller-identity", "--output", "json"]),
        "AWS caller identity",
    )
    account = caller.get("Account")
    if not isinstance(account, str) or not ACCOUNT_RE.fullmatch(account):
        raise AccountError("AWS caller identity returned an invalid account id")
    configured = runner.text(
        ["aws", "configure", "get", "sso_account_id", "--profile", profile]
    )
    if not ACCOUNT_RE.fullmatch(configured):
        raise AccountError(f"AWS profile {profile} has no 12-digit sso_account_id")
    if account != configured:
        raise AccountError("AWS caller account does not match the selected profile")
    arn = caller.get("Arn")
    if not isinstance(arn, str) or f":{account}:" not in arn:
        raise AccountError("AWS caller identity returned an invalid ARN")
    return {
        "profile": profile,
        "account_id": account,
        "caller_arn": arn,
        "account_matches_profile": True,
    }


def backend(account: str, runner: Runner) -> dict[str, Any]:
    bucket = f"mlp-tfstate-{account}"
    try:
        runner.run(["aws", "s3api", "head-bucket", "--bucket", bucket])
    except AccountError as error:
        raise AccountError(
            "state bucket could not be confirmed; inspect access and use the "
            "documented make aws-bootstrap path if the bucket is absent"
        ) from error
    versioning = object_value(
        runner.json(["aws", "s3api", "get-bucket-versioning", "--bucket", bucket]),
        "state bucket versioning",
    )
    encryption = object_value(
        runner.json(["aws", "s3api", "get-bucket-encryption", "--bucket", bucket]),
        "state bucket encryption",
    )
    public_access = object_value(
        runner.json(["aws", "s3api", "get-public-access-block", "--bucket", bucket]),
        "state bucket public access block",
    )
    rules = encryption.get("ServerSideEncryptionConfiguration", {}).get("Rules")
    algorithms = (
        {
            rule.get("ApplyServerSideEncryptionByDefault", {}).get("SSEAlgorithm")
            for rule in rules
            if isinstance(rule, dict)
        }
        if isinstance(rules, list)
        else set()
    )
    block = public_access.get("PublicAccessBlockConfiguration")
    block_keys = (
        "BlockPublicAcls",
        "BlockPublicPolicy",
        "IgnorePublicAcls",
        "RestrictPublicBuckets",
    )
    if versioning.get("Status") != "Enabled":
        raise AccountError("state bucket versioning is not enabled")
    if "AES256" not in algorithms:
        raise AccountError("state bucket does not use the expected AES256 encryption")
    if not isinstance(block, dict) or any(
        block.get(key) is not True for key in block_keys
    ):
        raise AccountError("state bucket public access block is incomplete")
    return {
        "bucket": bucket,
        "exists": True,
        "versioning": "Enabled",
        "encryption": "AES256",
        "public_access_blocked": True,
    }


def budget(account: str, runner: Runner) -> dict[str, Any]:
    value = object_value(
        runner.json(
            [
                "aws",
                "budgets",
                "describe-budget",
                "--account-id",
                account,
                "--budget-name",
                BUDGET_NAME,
                "--output",
                "json",
            ]
        ),
        "AWS budget",
    )
    budget_value = value.get("Budget")
    if not isinstance(budget_value, dict):
        raise AccountError("AWS budget response has no Budget object")
    if budget_value.get("BudgetName") != BUDGET_NAME:
        raise AccountError("AWS budget response names the wrong budget")
    if budget_value.get("BudgetType") != "COST":
        raise AccountError("AWS budget must have type COST")
    if budget_value.get("TimeUnit") != "MONTHLY":
        raise AccountError("AWS budget must have a MONTHLY period")
    limit = budget_value.get("BudgetLimit")
    if not isinstance(limit, dict) or limit.get("Unit") != "USD":
        raise AccountError("AWS budget does not have a USD limit")
    limit_usd = number(limit.get("Amount"), "budget limit")
    if limit_usd <= 0 or limit_usd > Decimal("5"):
        raise AccountError("AWS budget limit must be above zero and no greater than $5")
    notifications = list_value(
        runner.json(
            [
                "aws",
                "budgets",
                "describe-notifications-for-budget",
                "--account-id",
                account,
                "--budget-name",
                BUDGET_NAME,
                "--output",
                "json",
            ]
        ),
        "Notifications",
        "AWS budget notifications",
    )
    expected_notifications = {
        ("ACTUAL", "GREATER_THAN", Decimal("80"), "PERCENTAGE"),
        ("FORECASTED", "GREATER_THAN", Decimal("100"), "PERCENTAGE"),
    }
    notification_settings = set()
    subscribers = []
    for notification in notifications:
        if not isinstance(notification, dict):
            raise AccountError("AWS budget returned an invalid notification")
        identity = {
            key: notification.get(key)
            for key in (
                "NotificationType",
                "ComparisonOperator",
                "Threshold",
                "ThresholdType",
            )
        }
        notification_settings.add(
            (
                identity["NotificationType"],
                identity["ComparisonOperator"],
                number(identity["Threshold"], "budget notification threshold"),
                identity["ThresholdType"],
            )
        )
        current = list_value(
            runner.json(
                [
                    "aws",
                    "budgets",
                    "describe-subscribers-for-notification",
                    "--account-id",
                    account,
                    "--budget-name",
                    BUDGET_NAME,
                    "--notification",
                    json.dumps(identity, separators=(",", ":")),
                    "--output",
                    "json",
                ]
            ),
            "Subscribers",
            "AWS budget subscribers",
        )
        for subscriber in current:
            if not isinstance(subscriber, dict):
                raise AccountError("AWS budget returned an invalid subscriber")
            kind = subscriber.get("SubscriptionType")
            address = subscriber.get("Address")
            if (
                kind not in {"EMAIL", "SNS"}
                or not isinstance(address, str)
                or not address
            ):
                raise AccountError(
                    "AWS budget returned an invalid subscriber destination"
                )
            subscribers.append({"type": kind, "address": address})
    if notification_settings != expected_notifications:
        raise AccountError("AWS budget notification settings do not match Terraform")
    if not subscribers:
        raise AccountError("AWS budget has no notification subscriber")
    return {
        "name": BUDGET_NAME,
        "active": True,
        "limit_usd": decimal_text(limit_usd),
        "notification_count": len(notifications),
        "subscriber_count": len(subscribers),
        "subscribers": subscribers,
        "has_notification_subscriber": True,
    }


def service_quota(
    service_code: str, quota_code: str, runner: Runner
) -> tuple[str, Decimal]:
    value = object_value(
        runner.json(
            [
                "aws",
                "service-quotas",
                "get-service-quota",
                "--service-code",
                service_code,
                "--quota-code",
                quota_code,
                "--output",
                "json",
            ]
        ),
        f"{service_code} service quota",
    )
    quota = value.get("Quota")
    if not isinstance(quota, dict) or quota.get("QuotaCode") != quota_code:
        raise AccountError(f"{service_code} returned the wrong quota")
    name = quota.get("QuotaName")
    if not isinstance(name, str) or not name:
        raise AccountError(f"{service_code} quota has no name")
    return name, number(quota.get("Value"), f"{name} quota")


def quota_result(
    name: str, code: str, limit: Decimal, used: Decimal, required: Decimal
) -> dict[str, Any]:
    remaining = limit - used
    return {
        "name": name,
        "quota_code": code,
        "limit": decimal_text(limit),
        "used": decimal_text(used),
        "required": decimal_text(required),
        "remaining": decimal_text(remaining),
        "passed": remaining >= required,
    }


def eks_quota(runner: Runner) -> dict[str, Any]:
    quotas = list_value(
        runner.json(
            [
                "aws",
                "service-quotas",
                "list-service-quotas",
                "--service-code",
                "eks",
                "--output",
                "json",
            ]
        ),
        "Quotas",
        "EKS service quotas",
    )
    matches = [quota for quota in quotas if quota.get("QuotaName") == "Clusters"]
    if len(matches) != 1:
        raise AccountError(
            "EKS service quotas did not return exactly one Clusters quota"
        )
    quota = matches[0]
    code = quota.get("QuotaCode")
    if not isinstance(code, str) or not code.startswith("L-"):
        raise AccountError("EKS Clusters quota has an invalid code")
    clusters = list_value(
        runner.json(["aws", "eks", "list-clusters", "--output", "json"]),
        "clusters",
        "EKS clusters",
    )
    return quota_result(
        "EKS clusters",
        code,
        number(quota.get("Value"), "EKS Clusters quota"),
        Decimal(len(clusters)),
        Decimal("1"),
    )


def msk_quota(runner: Runner) -> dict[str, Any]:
    quota_values = list_value(
        runner.json(
            [
                "aws",
                "service-quotas",
                "list-service-quotas",
                "--service-code",
                "kafka",
                "--output",
                "json",
            ]
        ),
        "Quotas",
        "MSK service quotas",
    )
    matches = [
        quota
        for quota in quota_values
        if isinstance(quota, dict)
        and "serverless" in str(quota.get("QuotaName", "")).lower()
        and "cluster" in str(quota.get("QuotaName", "")).lower()
        and "account" in str(quota.get("QuotaName", "")).lower()
    ]
    if len(matches) > 1:
        raise AccountError("MSK service quotas returned multiple Serverless limits")
    if matches:
        name = matches[0].get("QuotaName")
        code = matches[0].get("QuotaCode")
        if not isinstance(name, str) or not isinstance(code, str):
            raise AccountError("MSK Serverless quota has invalid identity fields")
        limit = number(matches[0].get("Value"), "MSK Serverless cluster quota")
        limit_source = "service-quotas"
    else:
        name = "MSK Serverless clusters"
        code = "documented-default-limit"
        limit = Decimal("10")
        limit_source = SOURCES["msk_quotas"]
    clusters = list_value(
        runner.json(["aws", "kafka", "list-clusters-v2", "--output", "json"]),
        "ClusterInfoList",
        "MSK clusters",
    )
    used = sum(
        1
        for cluster in clusters
        if isinstance(cluster, dict) and cluster.get("ClusterType") == "SERVERLESS"
    )
    result = quota_result(name, code, limit, Decimal(used), Decimal("1"))
    result["limit_source"] = limit_source
    return result


def rds_quota(runner: Runner) -> dict[str, Any]:
    attributes = list_value(
        runner.json(["aws", "rds", "describe-account-attributes", "--output", "json"]),
        "AccountQuotas",
        "RDS account quotas",
    )
    matches = [
        item for item in attributes if item.get("AccountQuotaName") == "DBInstances"
    ]
    if len(matches) != 1:
        raise AccountError("RDS account quotas did not return DBInstances")
    quota = matches[0]
    return quota_result(
        "RDS DB instances",
        "DBInstances",
        number(quota.get("Max"), "RDS DBInstances maximum"),
        number(quota.get("Used"), "RDS DBInstances usage"),
        Decimal("1"),
    )


def spot_vcpu_usage(value: Any) -> tuple[Decimal, set[str]]:
    reservations = list_value(value, "Reservations", "EC2 Spot instances")
    total = Decimal("0")
    instance_ids: set[str] = set()
    for reservation in reservations:
        if not isinstance(reservation, dict):
            raise AccountError("EC2 returned an invalid Spot reservation")
        instances = reservation.get("Instances")
        if not isinstance(instances, list):
            raise AccountError("EC2 returned an invalid Spot instance list")
        for instance in instances:
            cpu = instance.get("CpuOptions") if isinstance(instance, dict) else None
            if not isinstance(cpu, dict):
                raise AccountError("EC2 Spot instance has no CPU options")
            cores = number(cpu.get("CoreCount"), "Spot instance core count")
            threads = number(cpu.get("ThreadsPerCore"), "Spot threads per core")
            total += cores * threads
            instance_id = instance.get("InstanceId")
            if not isinstance(instance_id, str) or not instance_id:
                raise AccountError("EC2 Spot instance has no instance id")
            instance_ids.add(instance_id)
    return total, instance_ids


def open_spot_vcpu_usage(value: Any, instance_ids: set[str], runner: Runner) -> Decimal:
    requests = list_value(value, "SpotInstanceRequests", "EC2 Spot requests")
    pending_types = []
    direct = Decimal("0")
    for request in requests:
        if not isinstance(request, dict):
            raise AccountError("EC2 returned an invalid Spot request")
        instance_id = request.get("InstanceId")
        if isinstance(instance_id, str) and instance_id in instance_ids:
            continue
        launch = request.get("LaunchSpecification")
        if not isinstance(launch, dict):
            raise AccountError("EC2 Spot request has no launch specification")
        cpu = launch.get("CpuOptions")
        if isinstance(cpu, dict):
            direct += number(cpu.get("CoreCount"), "Spot request core count") * number(
                cpu.get("ThreadsPerCore"), "Spot request threads per core"
            )
            continue
        instance_type = launch.get("InstanceType")
        if not isinstance(instance_type, str) or not instance_type:
            raise AccountError("EC2 Spot request has no instance type")
        pending_types.append(instance_type)
    if not pending_types:
        return direct
    descriptions = list_value(
        runner.json(
            [
                "aws",
                "ec2",
                "describe-instance-types",
                "--instance-types",
                *sorted(set(pending_types)),
                "--output",
                "json",
            ]
        ),
        "InstanceTypes",
        "EC2 instance types",
    )
    vcpus = {}
    for description in descriptions:
        if not isinstance(description, dict):
            raise AccountError("EC2 returned an invalid instance type")
        name = description.get("InstanceType")
        details = description.get("VCpuInfo")
        if not isinstance(name, str) or not isinstance(details, dict):
            raise AccountError("EC2 instance type has no vCPU information")
        vcpus[name] = number(details.get("DefaultVCpus"), f"{name} default vCPUs")
    if set(vcpus) != set(pending_types):
        raise AccountError("EC2 did not describe every open Spot request type")
    return direct + sum((vcpus[name] for name in pending_types), Decimal("0"))


def ec2_quotas(runner: Runner) -> list[dict[str, Any]]:
    spot_name, spot_limit = service_quota(
        "ec2", EC2_QUOTAS["spot_standard_vcpu"][0], runner
    )
    spot_used, spot_instance_ids = spot_vcpu_usage(
        runner.json(
            [
                "aws",
                "ec2",
                "describe-instances",
                "--filters",
                "Name=instance-lifecycle,Values=spot",
                "Name=instance-state-name,Values=pending,running",
                "--output",
                "json",
            ]
        )
    )
    spot_used += open_spot_vcpu_usage(
        runner.json(
            [
                "aws",
                "ec2",
                "describe-spot-instance-requests",
                "--filters",
                "Name=state,Values=open,active",
                "--output",
                "json",
            ]
        ),
        spot_instance_ids,
        runner,
    )
    eip_name, eip_limit = service_quota("ec2", EC2_QUOTAS["elastic_ip"][0], runner)
    eip_used = Decimal(
        len(
            list_value(
                runner.json(["aws", "ec2", "describe-addresses", "--output", "json"]),
                "Addresses",
                "Elastic IP addresses",
            )
        )
    )
    return [
        quota_result(
            spot_name,
            EC2_QUOTAS["spot_standard_vcpu"][0],
            spot_limit,
            spot_used,
            EC2_QUOTAS["spot_standard_vcpu"][1],
        ),
        quota_result(
            eip_name,
            EC2_QUOTAS["elastic_ip"][0],
            eip_limit,
            eip_used,
            EC2_QUOTAS["elastic_ip"][1],
        ),
    ]


def quotas(runner: Runner) -> dict[str, Any]:
    items = [
        eks_quota(runner),
        msk_quota(runner),
        rds_quota(runner),
        *ec2_quotas(runner),
    ]
    failures = [item["name"] for item in items if item["passed"] is not True]
    return {
        "items": items,
        "gate": {"passed": not failures, "failures": failures},
    }


def availability(runner: Runner, region: str) -> tuple[dict[str, Any], dict[str, Any]]:
    versions = list_value(
        runner.json(
            [
                "aws",
                "eks",
                "describe-cluster-versions",
                "--cluster-versions",
                EKS_VERSION,
                "--version-status",
                "STANDARD_SUPPORT",
                "--output",
                "json",
            ]
        ),
        "clusterVersions",
        "EKS cluster versions",
    )
    supported = [
        version
        for version in versions
        if isinstance(version, dict)
        and version.get("clusterVersion") == EKS_VERSION
        and (
            version.get("versionStatus") == "STANDARD_SUPPORT"
            or version.get("status") in {"STANDARD_SUPPORT", "standard-support"}
        )
    ]
    eks_passed = len(supported) == 1

    regions = {
        item.get("Value")
        for item in list_value(
            runner.json(
                [
                    "aws",
                    "ssm",
                    "get-parameters-by-path",
                    "--path",
                    "/aws/service/global-infrastructure/services/kafka/regions",
                    "--output",
                    "json",
                ]
            ),
            "Parameters",
            "MSK regional parameters",
        )
        if isinstance(item, dict)
    }
    msk_service_present = region in regions
    msk_documented = region in MSK_SERVERLESS_REGIONS
    msk_passed = msk_service_present and msk_documented

    options = list_value(
        runner.json(
            [
                "aws",
                "rds",
                "describe-orderable-db-instance-options",
                "--engine",
                "postgres",
                "--engine-version",
                "17.4",
                "--db-instance-class",
                "db.t4g.micro",
                "--vpc",
                "--output",
                "json",
            ]
        ),
        "OrderableDBInstanceOptions",
        "RDS orderable options",
    )
    rds_azs = {
        zone.get("Name")
        for option in options
        if isinstance(option, dict)
        and option.get("Engine") == "postgres"
        and option.get("EngineVersion") == "17.4"
        and option.get("DBInstanceClass") == "db.t4g.micro"
        and option.get("StorageType") == "gp3"
        and option.get("Vpc") is True
        and option.get("SupportsStorageEncryption") is True
        for zone in option.get("AvailabilityZones", [])
        if isinstance(zone, dict)
    }
    rds_passed = set(FIXED_AZS).issubset(rds_azs)

    offerings = list_value(
        runner.json(
            [
                "aws",
                "ec2",
                "describe-instance-type-offerings",
                "--location-type",
                "availability-zone",
                "--filters",
                "Name=instance-type,Values=t3.medium",
                "--output",
                "json",
            ]
        ),
        "InstanceTypeOfferings",
        "EC2 instance offerings",
    )
    ec2_azs = {
        item.get("Location")
        for item in offerings
        if isinstance(item, dict)
        and item.get("InstanceType") == "t3.medium"
        and item.get("LocationType") == "availability-zone"
    }
    ec2_passed = set(FIXED_AZS).issubset(ec2_azs)

    items = {
        "eks_standard_support": {
            "version": EKS_VERSION,
            "passed": eks_passed,
        },
        "msk_serverless_region": {
            "region": region,
            "service_region_present": msk_service_present,
            "documented_region_present": msk_documented,
            "documented_serverless_source": SOURCES["msk_serverless"],
            "passed": msk_passed,
        },
        "rds_postgres": {
            "engine_version": "17.4",
            "instance_class": "db.t4g.micro",
            "storage_type": "gp3",
            "availability_zones": sorted(rds_azs),
            "required_availability_zones": list(FIXED_AZS),
            "passed": rds_passed,
        },
        "ec2_nodes": {
            "instance_type": "t3.medium",
            "availability_zones": sorted(ec2_azs),
            "required_availability_zones": list(FIXED_AZS),
            "passed": ec2_passed,
        },
    }
    failures = [name for name, item in items.items() if item["passed"] is not True]
    eks = {
        "version": EKS_VERSION,
        "status": "STANDARD_SUPPORT" if eks_passed else "unproved",
        "standard_support": eks_passed,
        "source": SOURCES["eks_versions"],
    }
    return items | {"gate": {"passed": not failures, "failures": failures}}, eks


def collect(
    run_id: str, commit: str, profile: str, region: str, runner: Runner
) -> dict[str, Any]:
    if region != EXPECTED_REGION:
        raise AccountError(f"M4 account capture requires {EXPECTED_REGION}")
    require_clean_commit(commit, runner)
    repository = repository_identity(runner)
    identity = aws_identity(profile, runner)
    state = backend(identity["account_id"], runner)
    budget_value = budget(identity["account_id"], runner)
    quota_value = quotas(runner)
    availability_value, eks = availability(runner, region)
    failures = []
    if quota_value["gate"]["passed"] is not True:
        failures.append("quotas")
    if availability_value["gate"]["passed"] is not True:
        failures.append("availability")
    return {
        "schema_version": 1,
        "run_id": run_id,
        "source_commit": commit,
        "captured_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "region": region,
        "repository": repository,
        "aws": identity,
        "backend": state,
        "eks": eks,
        "budget": budget_value,
        "quotas": quota_value,
        "availability": availability_value,
        "sources": SOURCES,
        "gate": {"passed": not failures, "failures": failures},
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


def main() -> int:
    args = parse_args()
    try:
        output = validate_destination(args.run_id, args.output)
        require_preflight(args.run_id, args.commit)
        receipt = collect(
            args.run_id,
            args.commit,
            args.profile,
            args.region,
            Runner(),
        )
        write_json(output, receipt)
    except AccountError as error:
        print(f"account capture failed: {error}", file=sys.stderr)
        return 1
    if receipt["gate"]["passed"] is not True:
        failures = ", ".join(receipt["gate"]["failures"])
        print(f"account gate failed: {failures}", file=sys.stderr)
        return 1
    print(f"captured M4 account evidence: {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
