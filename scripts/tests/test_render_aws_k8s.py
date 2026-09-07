from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import unittest


SCRIPT = Path(__file__).parents[1] / "render-aws-k8s.py"
COMMIT = "a" * 40
RELAY = (
    "123456789012.dkr.ecr.us-east-1.amazonaws.com/mlp-dev/relay@sha256:"
    + "a" * 64
)
SINK = (
    "123456789012.dkr.ecr.us-east-1.amazonaws.com/mlp-dev/sink@sha256:"
    + "b" * 64
)
MSK = "boot-example.c1.kafka-serverless.us-east-1.amazonaws.com:9098"


def run_renderer(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        check=False,
        capture_output=True,
        text=True,
    )


class RendererTest(unittest.TestCase):
    def application_args(self) -> list[str]:
        return [
            "application",
            "--commit",
            COMMIT,
            "--relay-image",
            RELAY,
            "--sink-image",
            SINK,
            "--msk-bootstrap",
            MSK,
        ]

    def test_application_accepts_only_pinned_live_inputs(self):
        result = run_renderer(*self.application_args())

        self.assertEqual(result.returncode, 0, result.stderr)
        document = json.loads(result.stdout)
        self.assertEqual(document["spec"]["source"]["targetRevision"], COMMIT)
        self.assertEqual(document["spec"]["source"]["path"], "k8s/apps/aws")

    def test_application_refuses_unsafe_inputs(self):
        cases = {
            "short commit": ("--commit", "a" * 39),
            "mutable relay tag": (
                "--relay-image",
                "123456789012.dkr.ecr.us-east-1.amazonaws.com/mlp-dev/relay:v1",
            ),
            "swapped relay repository": (
                "--relay-image",
                SINK,
            ),
            "cross-account sink": (
                "--sink-image",
                "210987654321.dkr.ecr.us-east-1.amazonaws.com/mlp-dev/sink@sha256:"
                + "b" * 64,
            ),
            "plaintext broker port": (
                "--msk-bootstrap",
                "boot-example.c1.kafka-serverless.us-east-1.amazonaws.com:9092",
            ),
            "non-GitHub repository": (
                "--repo-url",
                "https://gitlab.com/lilabrooks/my-local-platform.git",
            ),
        }
        for name, (flag, unsafe) in cases.items():
            with self.subTest(name=name):
                args = self.application_args()
                if flag in args:
                    args[args.index(flag) + 1] = unsafe
                else:
                    args.extend([flag, unsafe])

                result = run_renderer(*args)

                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(result.stdout, "")
                self.assertNotEqual(result.stderr.strip(), "")

    def test_replay_validates_timestamp_and_preserves_evidence(self):
        base = [
            "replay",
            "--commit",
            COMMIT,
            "--relay-image",
            RELAY,
        ]
        result = run_renderer(*base, "--since", "2026-09-06T14:15:16Z")

        self.assertEqual(result.returncode, 0, result.stderr)
        document = json.loads(result.stdout)
        self.assertNotIn("ttlSecondsAfterFinished", document["spec"])
        args = document["spec"]["template"]["spec"]["containers"][0]["args"]
        self.assertIn("--since=2026-09-06T14:15:16Z", args)

        invalid = run_renderer(*base, "--since", "yesterday")
        self.assertNotEqual(invalid.returncode, 0)
        self.assertIn("RFC3339 UTC", invalid.stderr)

    def test_runtime_refuses_non_iam_broker_port(self):
        result = run_renderer(
            "runtime",
            "--msk-bootstrap",
            "boot-example.c1.kafka-serverless.us-east-1.amazonaws.com:9092",
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("port 9098", result.stderr)


if __name__ == "__main__":
    unittest.main()
