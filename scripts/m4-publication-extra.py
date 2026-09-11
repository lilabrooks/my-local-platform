#!/usr/bin/env python3
"""Publish historical staging and append-only recovery evidence, never deploy."""

from __future__ import annotations

import argparse
import datetime as dt
import importlib.util
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parent.parent


def module(name):
    spec = importlib.util.spec_from_file_location(
        name, Path(__file__).with_name(name + ".py")
    )
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


E = module("m4-evidence")
STAGE = module("m4-stage")
INVENTORY = module("m4-aws-inventory")
STAGING = {
    "preflight": "00-preflight.json",
    "identity": "01-identity.txt",
    "prices_markdown": "02-prices.md",
    "prices": "02-prices.json",
    "plan": "03-plan-summary.json",
    "inventory": "04-inventory-before.json",
    "images": "05-images.json",
    "capture_plan": "capture-plan.json",
}


def utc():
    return dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def safe(path):
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 8 * 1024 * 1024:
        raise E.EvidenceError("evidence input is missing or unsafe")
    return path


def destination(root, phase, run_id, observation=None):
    E.validate_run_id(run_id)
    path = root / "docs" / "evidence" / f"m4-{phase}" / run_id
    if observation:
        E.validate_run_id(observation)
        path /= observation
    E.ensure_beneath(path, root)
    return path


def verify_staging(root, run_id, redactions, output_root):
    raw, _ = E.evidence_paths(root, run_id)
    target = destination(output_root, "staging", run_id)
    receipt = E.read_json_object(target / "publication.json", "staging publication")
    packet = E.read_json_object(raw / "06-go-no-go.json", "GO")
    if (
        receipt.get("phase") != "staging"
        or receipt.get("result") != "staged"
        or receipt.get("run_id") != run_id
        or receipt.get("source_commit") != packet.get("source_commit")
        or receipt.get("execution_authorized") is not False
        or receipt.get("input_sha256") != packet.get("input_sha256")
    ):
        raise E.EvidenceError("staging receipt binding mismatch")
    names = set(STAGING.values()) | {"06-go-no-go.json"}
    if set(receipt.get("files", {})) != names or {
        p.name for p in target.iterdir()
    } != names | {"publication.json"}:
        raise E.EvidenceError("staging file manifest mismatch")
    if receipt.get("raw_sha256") != {
        name: E.hash_file(safe(raw / name)) for name in names
    }:
        raise E.EvidenceError("staging source changed after publication")
    for name in names:
        path = safe(target / name)
        if receipt["files"][name] != E.hash_file(path) or E.sensitive_matches(
            path.read_text(), redactions
        ):
            raise E.EvidenceError(
                "staging publication changed or contains sensitive data"
            )
    if E.sensitive_matches((target / "publication.json").read_text(), redactions):
        raise E.EvidenceError("staging receipt contains sensitive data")
    return target


def publish_staging(root, run_id, redactions_path, output_root):
    if root.resolve() == output_root.resolve():
        raise E.EvidenceError(
            "publish staging from a separate checkout to preserve the candidate"
        )
    raw, _ = E.evidence_paths(root, run_id)
    redactions = E.load_redactions(redactions_path)
    packet = E.read_json_object(raw / "06-go-no-go.json", "GO")
    # Preserve the dated staging decision, without claiming its inputs are
    # still fresh today. The shared binary plan may belong to a later run;
    # historical proof binds the recorded hash to this run's hashed summary.
    # Paid execution separately requires the exact binary and current ages.
    STAGE.ROOT = root
    STAGE.verify_go_receipt(
        run_id,
        packet["source_commit"],
        "us-east-1",
        raw / "06-go-no-go.json",
        raw / "03-plan-summary.json",
        now=E.parse_utc(packet["generated_at"], "GO time"),
    )
    names = set(STAGING.values()) | {"06-go-no-go.json"}
    for name in names:
        safe(raw / name)
    if (raw / "00-session.json").exists():
        E.scan_secret_files(
            raw, list(names), E.load_secret_scan(raw, run_id, complete=False)
        )
    target = destination(output_root, "staging", run_id)
    if target.exists():
        raise E.EvidenceError("staging publication already exists")
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=".staging-", dir=target.parent))
    try:
        for name in names:
            E.sanitize_one(raw / name, temporary / name, redactions)
        receipt = {
            "schema_version": 1,
            "phase": "staging",
            "run_id": run_id,
            "source_commit": packet["source_commit"],
            "result": "staged",
            "execution_authorized": False,
            "staged_at": packet["generated_at"],
            "published_at": utc(),
            "input_sha256": packet["input_sha256"],
            "raw_sha256": {name: E.hash_file(raw / name) for name in names},
            "files": {name: E.hash_file(temporary / name) for name in names},
        }
        E.write_json_exclusive(temporary / "publication.json", receipt, 0o644)
        os.rename(temporary, target)
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return verify_staging(root, run_id, redactions, output_root)


def recovery_path(root, run_id, observation):
    raw, _ = E.evidence_paths(root, run_id)
    E.validate_run_id(observation)
    path = raw / "recovery" / observation
    E.ensure_beneath(path, root)
    return raw, path


def expected_backend(account):
    return {
        "type": "s3",
        "bucket": f"mlp-tfstate-{account}",
        "key": "envs/dev/terraform.tfstate",
        "region": "us-east-1",
        "workspace": "default",
        "encrypt": True,
        "use_lockfile": True,
    }


def recovery_backend(root, account, profile):
    metadata = root / "infra/terraform/envs/dev/.terraform/terraform.tfstate"
    E.ensure_beneath(metadata, root)
    backend = E.read_json_object(safe(metadata), "backend metadata").get("backend", {})
    binding = expected_backend(account)
    config = backend.get("config", {})
    if backend.get("type") != "s3" or not isinstance(config, dict):
        raise E.EvidenceError("recovery requires the initialized dev S3 backend")
    required = {
        k: binding[k] for k in ("bucket", "key", "region", "encrypt", "use_lockfile")
    }
    allowed = {**required, "profile": profile, "workspace_key_prefix": "env:"}
    if any(config.get(k) != v for k, v in required.items()) or any(
        value not in (None, "", False, {}, [])
        and (key not in allowed or value != allowed[key])
        for key, value in config.items()
    ):
        raise E.EvidenceError(
            "recovery backend binding or credential/endpoint override mismatch"
        )
    return {**binding, "metadata_sha256": E.hash_file(metadata)}


def recovery_summary(root, run_id, observation):
    raw, path = recovery_path(root, run_id, observation)
    record = E.read_json_object(safe(path / "recovery-result.json"), "recovery result")
    prior = root / "docs/evidence/m4" / run_id / "publication.json"
    snapshot = raw / "failed-publication"
    if (
        record.get("run_id") != run_id
        or record.get("observation_id") != observation
        or record.get("source_commit") != E.validate_preflight(raw, run_id)["commit"]
        or record.get("original_publication_sha256") != E.hash_file(safe(prior))
        or record.get("controller_snapshot_sha256")
        != E.hash_file(safe(snapshot / "controller-state.json"))
        or record.get("session_sha256")
        != E.hash_file(safe(snapshot / "00-session.json"))
    ):
        raise E.EvidenceError("recovery does not match the original failed attempt")
    observed = E.parse_utc(record.get("observed_at"), "recovery observation")
    started = E.parse_utc(record.get("collection_started_at"), "recovery start")
    failed_at = E.parse_utc(
        E.read_json_object(snapshot / "observation.json", "failed observation")[
            "observed_at"
        ],
        "failed observation",
    )
    if not failed_at <= started <= observed:
        raise E.EvidenceError("recovery must follow the original failed observation")
    if observed > dt.datetime.now(dt.UTC):
        raise E.EvidenceError("recovery observation is in the future")
    if not isinstance(record.get("files"), dict) or not set(record["files"]) <= {
        "identity.json",
        "state.json",
        "inventory.json",
    }:
        raise E.EvidenceError("invalid recovery evidence manifest")
    for name, digest in record["files"].items():
        if E.hash_file(safe(path / name)) != digest:
            raise E.EvidenceError("recovery evidence changed")
    identity = (
        E.read_json_object(path / "identity.json", "recovery identity")
        if "identity.json" in record["files"]
        else {}
    )
    expected_account = E.read_json_object(snapshot / "01-identity.txt", "identity")[
        "aws"
    ]["account_id"]
    identity_ok = identity.get("Account") == expected_account
    state = (
        E.read_json_object(path / "state.json", "state check")
        if "state.json" in record["files"]
        else {}
    )
    inventory = (
        E.read_json_object(path / "inventory.json", "inventory check")
        if "inventory.json" in record["files"]
        else {}
    )
    counts = inventory.get("counts", {})
    resources = inventory.get("resources", {})
    services = set(INVENTORY.RUNTIME_SERVICES) | {"ecr"}
    inventory_empty = all(
        type(counts.get(k)) is int and counts[k] == 0 and resources.get(k) == []
        for k in services
    ) and (
        type(inventory.get("schema_version")) is int
        and inventory["schema_version"] == 1
        and inventory.get("region") == "us-east-1"
        and inventory.get("project") == "my-local-platform"
    )
    if inventory:
        captured = E.parse_utc(
            inventory.get("captured_at"), "recovery inventory capture"
        )
        if captured < started or not dt.timedelta(
            0
        ) <= observed - captured <= dt.timedelta(minutes=10):
            raise E.EvidenceError("recovery inventory is not fresh for the observation")
    if state:
        captured = E.parse_utc(state.get("captured_at"), "recovery state capture")
        if captured < started or not dt.timedelta(
            0
        ) <= observed - captured <= dt.timedelta(minutes=10):
            raise E.EvidenceError(
                "recovery state check is not fresh for the observation"
            )
    empty = (
        identity_ok
        and type(state.get("exit")) is int
        and state["exit"] == 0
        and state.get("resources") == []
        and isinstance(state.get("backend"), dict)
        and {k: state["backend"].get(k) for k in expected_backend(expected_account)}
        == expected_backend(expected_account)
        and re.fullmatch(
            r"[0-9a-f]{64}", str(state["backend"].get("metadata_sha256", ""))
        )
        is not None
        and inventory_empty
    )
    return {
        "schema_version": 1,
        "run_id": run_id,
        "observation_id": observation,
        "source_commit": record["source_commit"],
        "observed_at": record["observed_at"],
        "result": "recovered" if empty else "cleanup_unverified",
        "demonstration_passed": False,
        "identity_verified": identity_ok,
        "cleanup_verified": empty,
        "original_publication_sha256": record["original_publication_sha256"],
        "controller_snapshot_sha256": record["controller_snapshot_sha256"],
        "session_sha256": record["session_sha256"],
        "evidence_sha256": record["files"],
    }


def require_quiet_controller(raw):
    controller = E.read_json_object(raw / "controller-state.json", "controller")
    pid = controller.get("controller_pid")
    if type(pid) is not int or pid <= 0 or E.process_is_running(pid):
        raise E.EvidenceError("controller process status is active or unknown")
    try:
        processes = subprocess.run(
            ["ps", "-axo", "comm="], capture_output=True, timeout=10, check=True
        )
    except (OSError, subprocess.SubprocessError) as error:
        raise E.EvidenceError(
            "cannot establish local Terraform process status"
        ) from error
    if any(
        Path(p.strip()).name == "terraform"
        for p in processes.stdout.decode().splitlines()
    ):
        raise E.EvidenceError("a local Terraform process is still active")


def collect_recovery(root, run_id, observation, redactions_path):
    raw, path = recovery_path(root, run_id, observation)
    snapshot = raw / "failed-publication"
    E.verify_failed(root, run_id, E.load_redactions(redactions_path))
    require_quiet_controller(raw)
    path.mkdir(parents=True, mode=0o700, exist_ok=False)
    record = {
        "schema_version": 1,
        "run_id": run_id,
        "observation_id": observation,
        "source_commit": E.validate_preflight(raw, run_id)["commit"],
        "collection_started_at": utc(),
        "files": {},
        "original_publication_sha256": E.hash_file(
            root / "docs/evidence/m4" / run_id / "publication.json"
        ),
        "controller_snapshot_sha256": E.hash_file(
            raw / "failed-publication/controller-state.json"
        ),
        "session_sha256": E.hash_file(raw / "failed-publication/00-session.json"),
    }
    try:
        identity = E.private_aws_json(snapshot, ["sts", "get-caller-identity"])
        E.write_json_exclusive(path / "identity.json", identity, 0o600)
        expected = E.read_json_object(snapshot / "01-identity.txt", "identity")["aws"][
            "account_id"
        ]
        if identity.get("Account") != expected:
            raise E.EvidenceError("recovery account mismatch")
        profile = E.read_json_object(snapshot / "06-go-no-go.json", "GO").get(
            "aws_profile"
        )
        if not isinstance(profile, str) or not profile:
            raise E.EvidenceError("recovery profile binding missing")
        env = {k: os.environ[k] for k in ("HOME", "PATH", "TMPDIR") if k in os.environ}
        env.update(
            TF_WORKSPACE="default",
            TF_INPUT="0",
            AWS_PROFILE=profile,
            AWS_REGION="us-east-1",
            AWS_DEFAULT_REGION="us-east-1",
            AWS_MAX_ATTEMPTS="1",
            AWS_PAGER="",
        )
        backend = recovery_backend(root, expected, profile)
        result = subprocess.run(
            [
                "terraform",
                f"-chdir={root / 'infra/terraform/envs/dev'}",
                "state",
                "list",
            ],
            env=env,
            capture_output=True,
            timeout=45,
        )
        if recovery_backend(root, expected, profile) != backend:
            raise E.EvidenceError("backend changed during recovery observation")
        E.write_json_exclusive(
            path / "state.json",
            {
                "exit": result.returncode,
                "backend": backend,
                "captured_at": utc(),
                "resources": result.stdout.decode().splitlines()
                if result.returncode == 0
                else None,
            },
            0o600,
        )
        inventory = INVENTORY.collect(
            "us-east-1", runner=lambda args: E.private_aws_json(snapshot, args)
        )
        INVENTORY.require_cleanup_inventory(inventory, Path("21-inventory-after.json"))
        E.write_json_exclusive(path / "inventory.json", inventory, 0o600)
    except (
        E.EvidenceError,
        INVENTORY.InventoryError,
        OSError,
        ValueError,
        subprocess.SubprocessError,
    ):
        # Absence of a result stays unverified. Never include free-text diagnostics.
        pass
    record["observed_at"] = utc()
    record["files"] = {p.name: E.hash_file(p) for p in path.iterdir() if p.is_file()}
    E.write_json_exclusive(path / "recovery-result.json", record, 0o600)
    return recovery_summary(root, run_id, observation)


def publish_recovery(root, run_id, observation, redactions_path, *, verify=False):
    raw, path = recovery_path(root, run_id, observation)
    redactions = E.load_redactions(redactions_path)
    E.verify_failed(root, run_id, redactions)
    summary = recovery_summary(root, run_id, observation)
    target = destination(root, "recovery", run_id, observation)
    receipt = {
        "schema_version": 1,
        "phase": "recovery",
        "run_id": run_id,
        "recovery_receipt_sha256": E.hash_file(path / "recovery-result.json"),
    }
    if not verify:
        if target.exists():
            raise E.EvidenceError("recovery publication already exists")
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = Path(tempfile.mkdtemp(prefix=".recovery-", dir=target.parent))
        try:
            E.write_json_exclusive(temporary / "recovery.json", summary, 0o644)
            E.write_json_exclusive(temporary / "publication.json", receipt, 0o644)
            E.scan_secret_files(
                temporary,
                ["recovery.json", "publication.json"],
                E.load_secret_scan(raw / "failed-publication", run_id, complete=False),
            )
            for p in temporary.iterdir():
                if E.sensitive_matches(p.read_text(), redactions):
                    raise E.EvidenceError("recovery summary contains sensitive data")
            os.rename(temporary, target)
        except BaseException:
            shutil.rmtree(temporary, ignore_errors=True)
            raise
    if (
        {p.name for p in target.iterdir()} != {"recovery.json", "publication.json"}
        or E.read_json_object(safe(target / "recovery.json"), "recovery") != summary
        or E.read_json_object(safe(target / "publication.json"), "publication")
        != receipt
    ):
        raise E.EvidenceError("recovery publication changed")
    return target


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "action",
        choices=(
            "stage",
            "verify-stage",
            "collect-recovery",
            "publish-recovery",
            "verify-recovery",
        ),
    )
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--redactions-file", type=Path, required=True)
    parser.add_argument("--publication-root", type=Path)
    parser.add_argument("--observation-id")
    args = parser.parse_args()
    try:
        if args.action in {"stage", "verify-stage"}:
            if not args.publication_root:
                raise E.EvidenceError("--publication-root is required for staging")
            if args.action == "stage":
                result = publish_staging(
                    ROOT,
                    args.run_id,
                    args.redactions_file,
                    args.publication_root.resolve(),
                )
            else:
                result = verify_staging(
                    ROOT,
                    args.run_id,
                    E.load_redactions(args.redactions_file),
                    args.publication_root.resolve(),
                )
        else:
            if not args.observation_id:
                raise E.EvidenceError("--observation-id is required")
            if args.action == "collect-recovery":
                result = collect_recovery(
                    ROOT, args.run_id, args.observation_id, args.redactions_file
                )
            else:
                result = publish_recovery(
                    ROOT,
                    args.run_id,
                    args.observation_id,
                    args.redactions_file,
                    verify=args.action == "verify-recovery",
                )
        print(result)
    except (E.EvidenceError, STAGE.StageError, OSError, ValueError, KeyError):
        print(
            "Evidence operation failed; preserve private inputs and inspect the relevant checks. No recovery mutation was attempted."
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
