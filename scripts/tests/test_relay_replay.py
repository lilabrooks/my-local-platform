from __future__ import annotations

import os
from pathlib import Path
import subprocess
import tempfile
import time
import unittest


SCRIPT = Path(__file__).parents[1] / "relay-replay.sh"


class RelayReplayInterruptionTest(unittest.TestCase):
    def test_sigterm_stops_replay_and_restores_cluster_consumer(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binary_directory = root / "bin"
            binary_directory.mkdir()
            log = root / "kubectl.log"
            kubectl = binary_directory / "kubectl"
            kubectl.write_text(
                """#!/usr/bin/env python3
import os
from pathlib import Path
import sys
import time

arguments = sys.argv[1:]
with Path(os.environ["MLP_FAKE_LOG"]).open("a", encoding="utf-8") as output:
    output.write(" ".join(arguments) + "\\n")
if arguments[:2] == ["config", "current-context"]:
    print("mlp")
elif "exec" in arguments:
    time.sleep(20)
""",
                encoding="utf-8",
            )
            kubectl.chmod(0o755)
            environment = {
                **os.environ,
                "MODE": "cluster",
                "MINIKUBE_PROFILE": "mlp",
                "MLP_FAKE_LOG": str(log),
                "PATH": f"{binary_directory}:{os.environ['PATH']}",
                "TMPDIR": str(root),
            }
            process = subprocess.Popen(
                ["bash", str(SCRIPT)],
                cwd=SCRIPT.parent.parent,
                env=environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if log.exists() and " exec " in f" {log.read_text()} ":
                    break
                time.sleep(0.05)
            else:
                process.kill()
                self.fail("fake replay command did not start")

            started = time.monotonic()
            process.terminate()
            stdout, stderr = process.communicate(timeout=5)
            elapsed = time.monotonic() - started

            self.assertEqual(process.returncode, 143, (stdout, stderr))
            self.assertLess(elapsed, 5)
            calls = log.read_text().splitlines()
            pause = next(i for i, call in enumerate(calls) if "paused-replicas=0" in call)
            restore = next(
                i for i, call in enumerate(calls) if "paused-replicas-" in call
            )
            self.assertLess(pause, restore)
            self.assertEqual(list(root.glob("mlp-relay-replay.*")), [])


if __name__ == "__main__":
    unittest.main()
