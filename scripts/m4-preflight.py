#!/usr/bin/env python3
"""Run the account-independent checks required before M4 AWS staging."""

from __future__ import annotations

import argparse
from collections.abc import Iterable, Mapping
import datetime
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
M3_RECEIPT = ROOT / "docs" / "evidence" / "m3" / "20260905" / "closure.json"
HOURLY_FLAGS = ("enable_eks", "enable_msk", "enable_rds")
REQUIRED_COMMANDS = (
    "aws",
    "docker",
    "gh",
    "git",
    "go",
    "helm",
    "jq",
    "kubectl",
    "make",
    "minikube",
    "python3",
    "terraform",
)
RUN_ID_RE = re.compile(r"^[0-9]{8}T[0-9]{6}Z$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
LOCAL_ENDPOINT_RE = re.compile(
    r"localhost|127\.0\.0\.1|host\.minikube\.internal|"
    r"(?:^|[^a-z0-9.-])(?:kafka|postgres):(?:5432|9092|9094)(?:$|[^0-9])",
    re.IGNORECASE,
)
IMAGE_REFERENCE_RE = re.compile(
    r"^(?:[a-z0-9.-]+(?::[0-9]+)?/)?[a-z0-9._/-]+(?:"
    r"@sha256:[0-9a-f]{64}|:[a-zA-Z0-9][a-zA-Z0-9._-]*)$"
)
M3_COMMANDS = {
    "make lint",
    "make test",
    "make k8s-validate",
    "make smoke",
    "make smoke-traces",
    "make relay-demo",
}


class PreflightError(RuntimeError):
    """A preflight condition was not met."""


def command_output(args: list[str], *, cwd: Path = ROOT) -> str:
    result = subprocess.run(
        args,
        cwd=cwd,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise PreflightError(f"{' '.join(args)} failed: {detail}")
    return result.stdout


def validate_run_id(run_id: str) -> None:
    if not RUN_ID_RE.fullmatch(run_id):
        raise PreflightError("AWS_RUN_ID must use UTC YYYYMMDDTHHMMSSZ")
    try:
        parsed = datetime.datetime.strptime(run_id, "%Y%m%dT%H%M%SZ").replace(
            tzinfo=datetime.UTC
        )
    except ValueError as error:
        raise PreflightError("AWS_RUN_ID is not a real UTC date and time") from error
    if parsed.strftime("%Y%m%dT%H%M%SZ") != run_id:
        raise PreflightError("AWS_RUN_ID is not a canonical UTC timestamp")


def ensure_no_symlink(path: Path, stop: Path) -> None:
    current = path
    while current != stop:
        if current.is_symlink():
            raise PreflightError(f"evidence path contains a symlink: {current}")
        if stop not in current.parents:
            raise PreflightError(f"evidence path escapes the repository: {path}")
        current = current.parent


def check_evidence_roots(root: Path) -> tuple[Path, Path]:
    raw_root = root / ".evidence" / "m4"
    sanitized_root = root / "docs" / "evidence" / "m4"
    ensure_no_symlink(raw_root, root)
    ensure_no_symlink(sanitized_root, root)
    ignored = subprocess.run(
        ["git", "check-ignore", "--quiet", "--", ".evidence/m4/preflight-probe"],
        cwd=root,
        check=False,
    )
    if ignored.returncode != 0:
        raise PreflightError(f"raw evidence root is not ignored by git: {raw_root}")
    return raw_root, sanitized_root


def evidence_paths(
    root: Path, run_id: str, *, create: bool = False
) -> tuple[Path, Path]:
    validate_run_id(run_id)
    raw_root, sanitized_root = check_evidence_roots(root)
    raw = raw_root / run_id
    sanitized = sanitized_root / run_id
    ensure_no_symlink(raw, root)
    ensure_no_symlink(sanitized, root)
    if sanitized.exists() and not sanitized.is_dir():
        raise PreflightError(
            f"sanitized evidence destination is not a directory: {sanitized}"
        )
    if sanitized.exists() and any(sanitized.iterdir()):
        raise PreflightError(
            f"sanitized evidence destination is already populated: {sanitized}"
        )
    if create:
        if raw.exists():
            raise PreflightError(f"raw evidence run id already exists: {raw}")
        raw.mkdir(parents=True, mode=0o700, exist_ok=True)
        raw.chmod(0o700)
    return raw, sanitized


def parse_hcl_flags(path: Path) -> dict[str, bool]:
    values: dict[str, bool] = {}
    text = re.sub(r"(?m)(#|//).*?$", "", path.read_text(encoding="utf-8"))
    for flag in HOURLY_FLAGS:
        matches = re.findall(rf"(?m)^\s*{flag}\s*=\s*([^\s]+)", text)
        for value in matches:
            lowered = value.lower()
            if lowered not in {"true", "false"}:
                raise PreflightError(f"{path} has a non-boolean {flag} value")
            if flag in values and values[flag] != (lowered == "true"):
                raise PreflightError(f"{path} assigns {flag} more than once")
            values[flag] = lowered == "true"
    return values


def parse_json_flags(path: Path) -> dict[str, bool]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise PreflightError(f"invalid JSON variable file {path}: {error}") from error
    if not isinstance(payload, dict):
        raise PreflightError(f"Terraform variable file must contain an object: {path}")
    values: dict[str, bool] = {}
    for flag in HOURLY_FLAGS:
        if flag not in payload:
            continue
        if not isinstance(payload[flag], bool):
            raise PreflightError(f"{path} has a non-boolean {flag} value")
        values[flag] = payload[flag]
    return values


def flag_values(path: Path) -> dict[str, bool]:
    if path.suffix == ".json":
        return parse_json_flags(path)
    return parse_hcl_flags(path)


def active_variable_files(directory: Path) -> list[Path]:
    paths = [directory / "terraform.tfvars", directory / "terraform.tfvars.json"]
    paths.extend(sorted(directory.glob("*.auto.tfvars")))
    paths.extend(sorted(directory.glob("*.auto.tfvars.json")))
    return [path for path in paths if path.is_file()]


def argument_variable_files(
    value: str, terraform_directory: Path
) -> tuple[list[Path], dict[str, bool]]:
    try:
        arguments = shlex.split(value)
    except ValueError as error:
        raise PreflightError(f"invalid Terraform arguments: {error}") from error
    files: list[Path] = []
    flags: dict[str, bool] = {}
    index = 0
    while index < len(arguments):
        argument = arguments[index]
        item = ""
        if argument in {"-var", "-var-file"}:
            index += 1
            if index >= len(arguments):
                raise PreflightError(f"{argument} is missing its value")
            item = arguments[index]
        elif argument.startswith("-var="):
            item = argument.removeprefix("-var=")
            argument = "-var"
        elif argument.startswith("-var-file="):
            item = argument.removeprefix("-var-file=")
            argument = "-var-file"
        if argument == "-var-file":
            path = Path(item)
            if not path.is_absolute():
                path = terraform_directory / path
            if not path.is_file():
                raise PreflightError(f"Terraform variable file does not exist: {path}")
            files.append(path)
        elif argument == "-var":
            name, separator, raw = item.partition("=")
            if not separator:
                raise PreflightError("Terraform -var must use name=value")
            if name in HOURLY_FLAGS:
                lowered = raw.lower()
                if lowered not in {"true", "false"}:
                    raise PreflightError(
                        f"Terraform argument has a non-boolean {name} value"
                    )
                flags[name] = lowered == "true"
        index += 1
    return files, flags


def check_hourly_flags(root: Path, environment: Mapping[str, str]) -> None:
    terraform_directory = root / "infra" / "terraform" / "envs" / "dev"
    defaults = (terraform_directory / "variables.tf").read_text(encoding="utf-8")
    for flag in HOURLY_FLAGS:
        block = re.search(
            rf'variable\s+"{flag}"\s*\{{(?P<body>.*?)\n\}}',
            defaults,
            re.DOTALL,
        )
        if not block or not re.search(
            r"(?m)^\s*default\s*=\s*false\s*$", block.group("body")
        ):
            raise PreflightError(f"Terraform default for {flag} is not false")

    sources: list[tuple[str, dict[str, bool]]] = []
    for path in active_variable_files(terraform_directory):
        sources.append((str(path), flag_values(path)))
    sources.append(
        (
            "terraform.tfvars.example",
            flag_values(terraform_directory / "terraform.tfvars.example"),
        )
    )

    for name in ("AWS_TF_ARGS", "TF_CLI_ARGS", "TF_CLI_ARGS_plan", "TF_CLI_ARGS_apply"):
        value = environment.get(name, "")
        if not value:
            continue
        files, flags = argument_variable_files(value, terraform_directory)
        sources.append((name, flags))
        for path in files:
            sources.append((f"{name}:{path}", flag_values(path)))

    environment_flags: dict[str, bool] = {}
    for flag in HOURLY_FLAGS:
        name = f"TF_VAR_{flag}"
        if name not in environment:
            continue
        value = environment[name].lower()
        if value not in {"true", "false"}:
            raise PreflightError(f"{name} must be true or false")
        environment_flags[flag] = value == "true"
    sources.append(("environment", environment_flags))

    enabled = [
        f"{source}:{flag}"
        for source, values in sources
        for flag, value in values.items()
        if value
    ]
    if enabled:
        raise PreflightError("hourly AWS flags are enabled: " + ", ".join(enabled))


def nested_values(value: Any) -> Iterable[str]:
    if isinstance(value, str):
        yield value
        stripped = value.strip()
        if stripped.startswith(("{", "[")):
            try:
                embedded = json.loads(stripped)
            except json.JSONDecodeError:
                return
            yield from nested_values(embedded)
    elif isinstance(value, dict):
        for child in value.values():
            yield from nested_values(child)
    elif isinstance(value, list):
        for child in value:
            yield from nested_values(child)


def rendered_images(value: Any) -> Iterable[str]:
    if isinstance(value, dict):
        for key, child in value.items():
            if key == "image" and isinstance(child, str):
                yield child
            elif key == "images" and isinstance(child, list):
                for image in child:
                    if isinstance(image, str):
                        yield image.rsplit("=", maxsplit=1)[-1]
            yield from rendered_images(child)
    elif isinstance(value, list):
        for child in value:
            yield from rendered_images(child)
    elif isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith(("{", "[")):
            try:
                embedded = json.loads(stripped)
            except json.JSONDecodeError:
                return
            yield from rendered_images(embedded)


def image_is_pinned(image: str) -> bool:
    if not IMAGE_REFERENCE_RE.fullmatch(image):
        return False
    if "@sha256:" in image:
        return True
    tag = image.rsplit(":", maxsplit=1)[-1].lower()
    return any(character.isdigit() for character in tag) and tag not in {
        "dev",
        "edge",
        "latest",
        "main",
        "nightly",
        "stable",
    }


def validate_rendered_documents(documents: Mapping[str, Any]) -> None:
    for source, document in documents.items():
        for value in nested_values(document):
            if LOCAL_ENDPOINT_RE.search(value):
                raise PreflightError(
                    f"{source} contains a local-only endpoint: {value}"
                )
        images = list(rendered_images(document))
        if source in {"application", "replay"} and not images:
            raise PreflightError(f"{source} rendered no workload image")
        for image in images:
            if not image_is_pinned(image):
                raise PreflightError(f"{source} contains an unpinned image: {image}")


def check_tracked_aws_manifests(root: Path) -> list[str]:
    files = sorted((root / "k8s" / "aws").rglob("*.yaml"))
    files.extend(sorted((root / "k8s" / "apps" / "aws").rglob("*.yaml")))
    files.append(root / "k8s" / "monitoring-values-aws.yaml")
    checked: list[str] = []
    for path in files:
        text = path.read_text(encoding="utf-8")
        for line in text.splitlines():
            content = line.split("#", maxsplit=1)[0].strip()
            if not content:
                continue
            if LOCAL_ENDPOINT_RE.search(content):
                raise PreflightError(
                    f"{path} contains a local-only endpoint: {content}"
                )
            match = re.match(r"^image:\s*[\"']?([^\s\"']+)", content)
            if not match:
                continue
            image = match.group(1)
            if not image_is_pinned(image):
                raise PreflightError(f"{path} contains an unpinned image: {image}")
        checked.append(str(path.relative_to(root)))
    return checked


def render_documents(root: Path) -> dict[str, Any]:
    renderer = root / "scripts" / "render-aws-k8s.py"
    commit = "a" * 40
    relay = (
        "123456789012.dkr.ecr.us-east-1.amazonaws.com/mlp-dev/relay@sha256:" + "a" * 64
    )
    sink = (
        "123456789012.dkr.ecr.us-east-1.amazonaws.com/mlp-dev/sink@sha256:" + "b" * 64
    )
    broker = "boot-example.c1.kafka-serverless.us-east-1.amazonaws.com:9098"
    commands = {
        "application": [
            sys.executable,
            str(renderer),
            "application",
            "--commit",
            commit,
            "--relay-image",
            relay,
            "--sink-image",
            sink,
            "--msk-bootstrap",
            broker,
        ],
        "runtime": [
            sys.executable,
            str(renderer),
            "runtime",
            "--msk-bootstrap",
            broker,
        ],
        "replay": [
            sys.executable,
            str(renderer),
            "replay",
            "--commit",
            commit,
            "--relay-image",
            relay,
        ],
    }
    documents: dict[str, Any] = {}
    for name, command in commands.items():
        try:
            documents[name] = json.loads(command_output(command, cwd=root))
        except json.JSONDecodeError as error:
            raise PreflightError(
                f"{name} renderer returned invalid JSON: {error}"
            ) from error
    validate_rendered_documents(documents)
    return documents


def validate_destroy_recipe(output: str) -> None:
    required = (
        "aws sts get-caller-identity",
        "test -f infra/terraform/envs/dev/.terraform/terraform.tfstate",
        "terraform destroy",
        "terraform state pull",
    )
    positions = []
    for fragment in required:
        position = output.find(fragment)
        if position < 0:
            raise PreflightError(f"aws-down dry run omits: {fragment}")
        positions.append(position)
    if positions != sorted(positions):
        raise PreflightError(
            "aws-down does not check identity, destroy, and back up state in order"
        )
    if "terraform apply" in output:
        raise PreflightError("aws-down dry run contains terraform apply")


def check_m3_receipt(root: Path) -> dict[str, Any]:
    path = root / M3_RECEIPT.relative_to(ROOT)
    if not path.is_file():
        raise PreflightError(f"M3 closure evidence is missing: {path}")
    try:
        receipt = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise PreflightError(f"M3 closure evidence is invalid JSON: {error}") from error
    if not isinstance(receipt, dict):
        raise PreflightError("M3 closure evidence must be a JSON object")
    closure = receipt.get("closure", {})
    commands = receipt.get("commands", [])
    measurements = receipt.get("measurements", {})
    records = receipt.get("recorded_in", [])
    if not isinstance(closure, dict):
        raise PreflightError("M3 closure evidence is incomplete")
    commit = closure.get("merge_commit", "")
    if (
        receipt.get("schema_version") != 1
        or receipt.get("milestone") != "M3"
        or receipt.get("result") != "passed"
        or closure.get("issue") != 90
        or closure.get("pull_request") != 113
        or not isinstance(commit, str)
        or not COMMIT_RE.fullmatch(commit)
        or not isinstance(commands, list)
        or not M3_COMMANDS.issubset(commands)
        or not isinstance(measurements, dict)
        or measurements.get("produced_events") != 600
        or measurements.get("peak_lag") != 598
        or measurements.get("peak_consumers") != 12
        or measurements.get("final_lag") != 0
        or measurements.get("final_consumers") != 1
        or not isinstance(records, list)
        or not records
    ):
        raise PreflightError("M3 closure evidence is incomplete")
    for recorded in records:
        record_path = Path(recorded) if isinstance(recorded, str) else Path("..")
        if (
            not isinstance(recorded, str)
            or record_path.is_absolute()
            or ".." in record_path.parts
            or not (root / record_path).is_file()
        ):
            raise PreflightError(f"M3 evidence points to a missing record: {recorded}")
    ancestor = subprocess.run(
        ["git", "merge-base", "--is-ancestor", commit, "HEAD"],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    if ancestor.returncode != 0:
        raise PreflightError(f"M3 evidence commit is not an ancestor of HEAD: {commit}")
    return receipt


def check_repository(root: Path, environment: Mapping[str, str]) -> dict[str, Any]:
    receipt = check_m3_receipt(root)
    check_hourly_flags(root, environment)
    raw_evidence, sanitized_evidence = check_evidence_roots(root)
    documents = render_documents(root)
    tracked_manifests = check_tracked_aws_manifests(root)
    destroy = command_output(["make", "--dry-run", "aws-down"], cwd=root)
    validate_destroy_recipe(destroy)
    return {
        "m3_merge_commit": receipt["closure"]["merge_commit"],
        "evidence_roots": {
            "raw": str(raw_evidence.relative_to(root)),
            "sanitized": str(sanitized_evidence.relative_to(root)),
        },
        "rendered_documents": sorted(documents),
        "tracked_aws_manifests": tracked_manifests,
        "destroy_command": "make aws-down",
    }


def require_commands() -> None:
    missing = [name for name in REQUIRED_COMMANDS if shutil.which(name) is None]
    if missing:
        raise PreflightError("required command(s) missing: " + ", ".join(missing))


def check_clean_head(root: Path) -> str:
    commit = command_output(["git", "rev-parse", "HEAD"], cwd=root).strip()
    if not COMMIT_RE.fullmatch(commit):
        raise PreflightError(f"HEAD is not a full lowercase commit SHA: {commit}")
    dirty = command_output(
        ["git", "status", "--porcelain", "--untracked-files=all"], cwd=root
    ).strip()
    if dirty:
        raise PreflightError("worktree must be clean before preflight:\n" + dirty)
    return commit


def validate_image_metadata(
    labels_text: str, image_id: str, image: str, commit: str
) -> dict[str, str]:
    try:
        labels = json.loads(labels_text)
    except json.JSONDecodeError as error:
        raise PreflightError(f"docker returned invalid labels for {image}") from error
    revision = (
        labels.get("org.opencontainers.image.revision")
        if isinstance(labels, dict)
        else None
    )
    if revision != commit:
        raise PreflightError(f"{image} was built from {revision!r}, expected {commit}")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", image_id):
        raise PreflightError(f"{image} has an invalid image id: {image_id}")
    return {"id": image_id, "revision": revision}


def inspect_image(image: str, commit: str) -> dict[str, str]:
    output = command_output(
        [
            "docker",
            "image",
            "inspect",
            "--format",
            "{{json .Config.Labels}}|{{.Id}}",
            image,
        ]
    ).strip()
    labels_text, separator, image_id = output.partition("|")
    if not separator:
        raise PreflightError(f"docker returned invalid metadata for {image}")
    return validate_image_metadata(labels_text, image_id, image, commit)


def write_receipt(path: Path, payload: dict[str, Any]) -> None:
    temporary = path.with_suffix(".tmp")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    descriptor = os.open(temporary, flags, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            json.dump(payload, output, indent=2, sort_keys=True)
            output.write("\n")
        temporary.replace(path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def run_step(
    name: str,
    command: list[str],
    checks: list[dict[str, str]],
    environment: Mapping[str, str] | None = None,
) -> None:
    print(f"==> {name}", flush=True)
    result = subprocess.run(command, cwd=ROOT, env=environment, check=False)
    status = "passed" if result.returncode == 0 else "failed"
    checks.append({"name": name, "command": shlex.join(command), "result": status})
    if result.returncode != 0:
        raise PreflightError(f"{name} failed with exit code {result.returncode}")


def run_preflight(root: Path, run_id: str, environment: Mapping[str, str]) -> Path:
    raw, sanitized = evidence_paths(root, run_id, create=True)
    receipt_path = raw / "00-preflight.json"
    if receipt_path.exists():
        raise PreflightError(f"preflight receipt already exists: {receipt_path}")

    started = datetime.datetime.now(datetime.UTC).replace(microsecond=0)
    checks: list[dict[str, str]] = []
    images: dict[str, dict[str, str]] = {}
    payload: dict[str, Any] = {
        "schema_version": 1,
        "run_id": run_id,
        "commit": None,
        "started_at": started.isoformat().replace("+00:00", "Z"),
        "result": "failed",
        "repository": {},
        "evidence": {
            "raw": str(raw.relative_to(root)),
            "sanitized": str(sanitized.relative_to(root)),
        },
        "checks": checks,
        "images": images,
    }
    try:
        require_commands()
        checks.append(
            {
                "name": "required local tools",
                "command": "PATH lookup",
                "result": "passed",
            }
        )
        repository = check_repository(root, environment)
        payload["repository"] = repository
        checks.append(
            {
                "name": "account-independent repository contract",
                "command": "make aws-preflight-check",
                "result": "passed",
            }
        )
        commit = check_clean_head(root)
        payload["commit"] = commit
        checks.append(
            {"name": "clean source commit", "command": "git status", "result": "passed"}
        )
        lint_environment = dict(environment)
        lint_environment.update({"LINT_STRICT": "1", "LINT_SKIP_OK": "golangci-lint"})
        run_step("repository lint", ["make", "lint"], checks, lint_environment)
        run_step("local tests", ["make", "test"], checks)
        run_step(
            "M4 workload images",
            ["make", "m4-images", f"M4_SOURCE_COMMIT={commit}"],
            checks,
        )
        images["relay:dev"] = inspect_image("relay:dev", commit)
        images["sink:dev"] = inspect_image("sink:dev", commit)
        for stack in ("infra/terraform/bootstrap", "infra/terraform/envs/dev"):
            run_step(
                f"Terraform init ({stack})",
                [
                    "terraform",
                    f"-chdir={stack}",
                    "init",
                    "-backend=false",
                    "-input=false",
                ],
                checks,
            )
            run_step(
                f"Terraform validate ({stack})",
                ["terraform", f"-chdir={stack}", "validate"],
                checks,
            )
        run_step(
            "Terraform contract tests",
            ["terraform", "-chdir=infra/terraform/envs/dev", "test"],
            checks,
        )
        run_step("rendered Kubernetes validation", ["make", "k8s-validate"], checks)
        payload["result"] = "passed"
    except BaseException as error:
        payload["error"] = str(error)
        raise
    finally:
        finished = datetime.datetime.now(datetime.UTC).replace(microsecond=0)
        payload["finished_at"] = finished.isoformat().replace("+00:00", "Z")
        write_receipt(receipt_path, payload)
    return receipt_path


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    subparsers = result.add_subparsers(dest="command", required=True)
    subparsers.add_parser(
        "check-repository",
        help="run the account- and cluster-independent repository checks",
    )
    run = subparsers.add_parser("run", help="run the complete local preflight")
    run.add_argument("--run-id", default=os.environ.get("AWS_RUN_ID", ""))
    return result


def main() -> int:
    args = parser().parse_args()
    try:
        if args.command == "check-repository":
            result = check_repository(ROOT, os.environ)
            print(json.dumps(result, indent=2, sort_keys=True))
        else:
            receipt = run_preflight(ROOT, args.run_id, os.environ)
            print(f"preflight passed; receipt: {receipt.relative_to(ROOT)}")
    except PreflightError as error:
        print(f"preflight: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
