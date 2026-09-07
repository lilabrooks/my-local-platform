#!/usr/bin/env python3
"""Render the untracked AWS ArgoCD root Application or replay Job."""

from __future__ import annotations

import argparse
import datetime
import json
import re
import sys
from typing import Any


COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
ECR_IMAGE_RE = re.compile(
    r"^(?P<account>[0-9]{12})\.dkr\.ecr\.us-east-1\.amazonaws\.com/"
    r"mlp-dev/(?P<name>relay|sink)@sha256:[0-9a-f]{64}$"
)
MSK_RE = re.compile(
    r"^[a-zA-Z0-9.-]+\.kafka-serverless\.us-east-1\.amazonaws\.com:9098"
    r"(?:,[a-zA-Z0-9.-]+\.kafka-serverless\.us-east-1\.amazonaws\.com:9098)*$"
)
REPO_RE = re.compile(r"^https://github\.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+(?:\.git)?$")


def fail(message: str) -> None:
    raise SystemExit(message)


def image(value: str, name: str) -> tuple[str, str]:
    match = ECR_IMAGE_RE.fullmatch(value)
    if not match or match.group("name") != name:
        fail(
            f"{name} image must be the us-east-1 mlp-dev/{name} ECR repository "
            "selected by a sha256 digest"
        )
    return value, match.group("account")


def inputs(args: argparse.Namespace) -> tuple[str, str]:
    if not COMMIT_RE.fullmatch(args.commit):
        fail("commit must be a full lowercase 40-character git SHA")
    relay_image, relay_account = image(args.relay_image, "relay")
    if getattr(args, "sink_image", None):
        _, sink_account = image(args.sink_image, "sink")
        if sink_account != relay_account:
            fail("relay and sink images must come from the same ECR registry")
    return relay_image, relay_account


def root_application(args: argparse.Namespace) -> dict[str, Any]:
    relay_image, _ = inputs(args)
    sink_image, _ = image(args.sink_image, "sink")
    if not MSK_RE.fullmatch(args.msk_bootstrap):
        fail("MSK bootstrap must be one or more us-east-1 Serverless IAM brokers on port 9098")
    if not REPO_RE.fullmatch(args.repo_url):
        fail("repo URL must be an HTTPS GitHub repository URL")

    revision_patch = json.dumps(
        [
            {"op": "replace", "path": "/spec/source/repoURL", "value": args.repo_url},
            {"op": "replace", "path": "/spec/source/targetRevision", "value": args.commit},
        ],
        separators=(",", ":"),
    )
    relay_source = {
        "images": [
            "example.invalid/mlp-dev/relay=" + relay_image,
        ],
        "patches": [
            {
                "target": {
                    "group": "keda.sh",
                    "version": "v1alpha1",
                    "kind": "ScaledObject",
                    "name": "relay-deliver",
                },
                "patch": json.dumps(
                    [
                        {
                            "op": "replace",
                            "path": "/spec/triggers/0/metadata/bootstrapServers",
                            "value": args.msk_bootstrap,
                        }
                    ],
                    separators=(",", ":"),
                ),
            }
        ],
    }
    return {
        "apiVersion": "argoproj.io/v1alpha1",
        "kind": "Application",
        "metadata": {
            "name": "root",
            "namespace": "argocd",
            "annotations": {
                "mlp.dev/runtime-config-map": "relay-runtime",
                "mlp.dev/runtime-secret": "relay-secrets",
            },
            "finalizers": ["resources-finalizer.argocd.argoproj.io"],
        },
        "spec": {
            "project": "mlp-root",
            "source": {
                "repoURL": args.repo_url,
                "targetRevision": args.commit,
                "path": "k8s/apps/aws",
                "kustomize": {
                    "patches": [
                        {
                            "target": {"group": "argoproj.io", "kind": "Application"},
                            "patch": revision_patch,
                        },
                        {
                            "target": {
                                "group": "argoproj.io",
                                "kind": "Application",
                                "name": "relay",
                            },
                            "patch": json.dumps(
                                [
                                    {
                                        "op": "add",
                                        "path": "/spec/source/kustomize",
                                        "value": relay_source,
                                    }
                                ],
                                separators=(",", ":"),
                            ),
                        },
                        {
                            "target": {
                                "group": "argoproj.io",
                                "kind": "Application",
                                "name": "sink",
                            },
                            "patch": json.dumps(
                                [
                                    {
                                        "op": "add",
                                        "path": "/spec/source/kustomize",
                                        "value": {
                                            "images": [
                                                "example.invalid/mlp-dev/sink=" + sink_image
                                            ]
                                        },
                                    }
                                ],
                                separators=(",", ":"),
                            ),
                        },
                    ]
                },
            },
            "destination": {
                "server": "https://kubernetes.default.svc",
                "namespace": "argocd",
            },
            "syncPolicy": {"automated": {"prune": True, "selfHeal": True}},
        },
    }


def replay_job(args: argparse.Namespace) -> dict[str, Any]:
    relay_image, _ = inputs(args)
    if args.since != "earliest":
        try:
            datetime.datetime.strptime(args.since, "%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            fail("since must be earliest or an RFC3339 UTC timestamp")
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "generateName": "relay-replay-",
            "namespace": "mlp",
            "annotations": {"mlp.dev/source-commit": args.commit},
            "labels": {
                "app.kubernetes.io/name": "relay-replay",
                "app.kubernetes.io/part-of": "my-local-platform",
            },
        },
        "spec": {
            "backoffLimit": 0,
            "activeDeadlineSeconds": 90,
            "template": {
                "metadata": {
                    "labels": {
                        "app.kubernetes.io/name": "relay-replay",
                        "app.kubernetes.io/part-of": "my-local-platform",
                    }
                },
                "spec": {
                    "serviceAccountName": "relay-deliver",
                    "restartPolicy": "Never",
                    "securityContext": {
                        "runAsNonRoot": True,
                        "runAsUser": 65532,
                        "seccompProfile": {"type": "RuntimeDefault"},
                    },
                    "containers": [
                        {
                            "name": "replay",
                            "image": relay_image,
                            "imagePullPolicy": "IfNotPresent",
                            "command": ["/relay-replay"],
                            "args": [
                                "--group=relay-deliver",
                                "--topic=mlp.relay.deliveries",
                                "--since=" + args.since,
                                "--wait=30s",
                            ],
                            "envFrom": [{"configMapRef": {"name": "relay-runtime"}}],
                            "securityContext": {
                                "allowPrivilegeEscalation": False,
                                "readOnlyRootFilesystem": True,
                                "capabilities": {"drop": ["ALL"]},
                            },
                            "resources": {
                                "requests": {"cpu": "10m", "memory": "32Mi"},
                                "limits": {"memory": "128Mi"},
                            },
                        }
                    ],
                },
            },
        },
    }


def runtime_config(args: argparse.Namespace) -> dict[str, Any]:
    if not MSK_RE.fullmatch(args.msk_bootstrap):
        fail("MSK bootstrap must be one or more us-east-1 Serverless IAM brokers on port 9098")
    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "relay-runtime",
            "namespace": "mlp",
            "labels": {"app.kubernetes.io/part-of": "my-local-platform"},
        },
        "data": {
            "KAFKA_BOOTSTRAP": args.msk_bootstrap,
            "KAFKA_AUTH_MODE": "aws_msk_iam",
            "AWS_REGION": "us-east-1",
            "DEPLOYMENT_ENVIRONMENT": "aws",
            "RELAY_TOPIC": "mlp.relay.deliveries",
            "RELAY_DLQ_TOPIC": "mlp.relay.deliveries.dlq",
            "RELAY_CONSUMER_GROUP": "relay-deliver",
            "RELAY_RETRY_DELAYS": "demo",
            "RELAY_DELIVERY_TIMEOUT": "2s",
            "RELAY_RETRY_JITTER": "true",
            "OTEL_EXPORTER_OTLP_ENDPOINT": "http://otel-collector:4317",
        },
    }


def parser() -> argparse.ArgumentParser:
    out = argparse.ArgumentParser(description=__doc__)
    sub = out.add_subparsers(dest="command", required=True)
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--commit", required=True)
    common.add_argument("--relay-image", required=True)

    root = sub.add_parser("application", parents=[common])
    root.add_argument("--sink-image", required=True)
    root.add_argument("--msk-bootstrap", required=True)
    root.add_argument(
        "--repo-url",
        default="https://github.com/lilabrooks/my-local-platform.git",
    )

    replay = sub.add_parser("replay", parents=[common])
    replay.add_argument("--since", default="earliest")

    runtime = sub.add_parser("runtime")
    runtime.add_argument("--msk-bootstrap", required=True)
    return out


def main() -> None:
    args = parser().parse_args()
    if args.command == "application":
        document = root_application(args)
    elif args.command == "replay":
        document = replay_job(args)
    else:
        document = runtime_config(args)
    json.dump(document, sys.stdout, indent=2)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
